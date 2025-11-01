import os
import shutil
import subprocess
import tempfile
import urllib.parse
import urllib.request
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, Sequence

from ru_address.common import Common
from ru_address.core import Core
from ru_address.dump import ConverterRegistry as DumpConverterRegistry
from ru_address.output import OutputRegistry
from ru_address.storage import resolve_storage


@dataclass(frozen=True)
class DatabaseConfig:
    dsn: str | None = None
    host: str | None = None
    port: int | None = None
    user: str | None = None
    password: str | None = None
    database: str | None = None


@dataclass(frozen=True)
class PipelineOptions:
    source: str
    tables: Sequence[str]
    regions: Sequence[str]
    jobs: int | None = None
    keep_zip: bool = False


def download_archive(source: str, workdir: str) -> tuple[str, bool]:
    parsed = urllib.parse.urlparse(source)
    if parsed.scheme in ('http', 'https', 'ftp'):
        filename = os.path.basename(parsed.path) or 'ru_address.zip'
        destination = os.path.join(workdir, filename)
        Common.cli_output(f'Downloading archive from {source}')
        urllib.request.urlretrieve(source, destination)
        return destination, True
    expanded = os.path.expanduser(source)
    if not os.path.exists(expanded):
        raise FileNotFoundError(f'Source archive not found: {source}')
    return os.path.abspath(expanded), False


def _build_psql_command(config: DatabaseConfig, dump_path: str) -> tuple[list[str], dict[str, str]]:
    command = ['psql', '--quiet', '--file', dump_path]
    env = os.environ.copy()
    if config.password:
        env['PGPASSWORD'] = config.password
    if config.dsn:
        command.extend(['--dbname', config.dsn])
        return command, env
    if config.database:
        command.extend(['--dbname', config.database])
    if config.host:
        command.extend(['--host', config.host])
    if config.port:
        command.extend(['--port', str(config.port)])
    if config.user:
        command.extend(['--username', config.user])
    return command, env


def _import_dump(config: DatabaseConfig, dump_path: str):
    command, env = _build_psql_command(config, dump_path)
    Common.cli_output(f'Running psql for dump {dump_path}')
    result = subprocess.run(command, capture_output=True, text=True, env=env)
    if result.stderr:
        Common.cli_output(result.stderr.strip())
    if result.returncode != 0:
        raise RuntimeError(f'psql exited with code {result.returncode}')


def _process_common_tables(zip_path: str, tables: Sequence[str], db_config: DatabaseConfig):
    common_tables = [table for table in tables if table in Core.COMMON_TABLE_LIST]
    if not common_tables:
        return
    Common.cli_output('Processing common tables')
    with tempfile.TemporaryDirectory() as tmpdir:
        dump_destination = os.path.join(tmpdir, 'common.sql')
        with resolve_storage(zip_path) as storage:
            converter = DumpConverterRegistry.init_converter('psql', storage, storage)
            output = OutputRegistry.init_output('direct', converter, dump_destination, include_meta=True)
            output.write(common_tables, [])
        _import_dump(db_config, dump_destination)


def _collect_regions(zip_path: str, requested: Sequence[str]) -> list[str]:
    with resolve_storage(zip_path) as storage:
        available = storage.list_regions()
    if not requested:
        return available
    requested_set = {region for region in requested}
    missing = sorted(requested_set.difference(available))
    if missing:
        raise ValueError(f'Requested regions not found in archive: {", ".join(missing)}')
    return [region for region in available if region in requested_set]


def _filter_region_tables(tables: Sequence[str]) -> list[str]:
    return [table for table in tables if table in Core.REGION_TABLE_LIST]


def _process_region(zip_path: str, region: str, tables: Sequence[str], db_config: DatabaseConfig):
    region_tables = _filter_region_tables(tables)
    if not region_tables:
        return
    Common.cli_output(f'Processing region {region}')
    failed_dump_path = os.path.join(os.getcwd(), f'{region}_failed.sql')
    with tempfile.TemporaryDirectory() as tmpdir:
        with resolve_storage(zip_path) as storage:
            converter = DumpConverterRegistry.init_converter('psql', storage, storage)
            output = OutputRegistry.init_output('per_region', converter, tmpdir, include_meta=True)
            output.write(region_tables, [region])
        dump_path = os.path.join(tmpdir, f'{region}.{converter.get_extension()}')
        try:
            _import_dump(db_config, dump_path)
        except Exception:
            if os.path.exists(dump_path):
                shutil.copy(dump_path, failed_dump_path)
            raise


def _run_region_pool(zip_path: str, regions: Iterable[str], tables: Sequence[str],
                     db_config: DatabaseConfig, jobs: int | None):
    region_tables = _filter_region_tables(tables)
    if not region_tables:
        return
    executor_workers = jobs or os.cpu_count() or 1
    Common.cli_output(f'Starting pool with {executor_workers} workers')
    with ProcessPoolExecutor(max_workers=executor_workers) as executor:
        futures = {
            executor.submit(_process_region, zip_path, region, region_tables, db_config): region
            for region in regions
        }
        for future in as_completed(futures):
            region = futures[future]
            try:
                future.result()
                Common.cli_output(f'Region {region} processed successfully')
            except Exception as exc:
                Common.cli_output(f'Region {region} failed: {exc}')
                raise


def execute_pipeline(options: PipelineOptions, db_config: DatabaseConfig):
    with tempfile.TemporaryDirectory() as workdir:
        archive_path, downloaded = download_archive(options.source, workdir)
        archive_path = os.path.abspath(archive_path)
        if not archive_path.lower().endswith('.zip'):
            raise ValueError('Pipeline expects source archive in ZIP format')
        _process_common_tables(archive_path, options.tables, db_config)
        regions = _collect_regions(archive_path, options.regions)
        _run_region_pool(archive_path, regions, options.tables, db_config, options.jobs)
        if downloaded and options.keep_zip:
            destination = os.path.join(os.getcwd(), os.path.basename(archive_path))
            if not os.path.exists(destination):
                shutil.copy(archive_path, destination)
            Common.cli_output(f'Archive preserved at {destination}')
