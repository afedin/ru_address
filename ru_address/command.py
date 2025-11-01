import os
import time
from contextlib import ExitStack
from functools import update_wrapper
import click
from ru_address.common import Common
from ru_address import __version__
from ru_address.core import Core
from ru_address.errors import UnknownPlatformError
from ru_address.output import OutputRegistry
from ru_address.schema import ConverterRegistry as SchemaConverterRegistry
from ru_address.dump import ConverterRegistry as DumpConverterRegistry
from ru_address.storage import resolve_storage
from ru_address.pipeline import execute_pipeline, DatabaseConfig, PipelineOptions


def command_summary(f):
    def wrapper(**kwargs):
        start_time = time.time()
        f(**kwargs)
        Common.show_memory_usage()
        Common.show_execution_time(start_time)
    return update_wrapper(wrapper, f)


@click.group(invoke_without_command=True, no_args_is_help=True)
@click.version_option(__version__)
@click.option("-e", "--env", type=(str, str), multiple=True, help='Pass ENV params')
@click.pass_context
def cli(_, env):
    for k, v in env:
        os.environ.setdefault(k, v)


@cli.command()
@click.option('--target', type=click.Choice(SchemaConverterRegistry.get_available_platforms_list()),
              default='mysql', help='Target schema format')
@click.option('-t', '--table', 'tables', type=str, multiple=True,
              default=Core.get_known_tables().keys(), help='Limit table list to process')
@click.option('--no-keys', is_flag=True, help='Exclude keys && column index')
@click.argument('source_path', type=click.types.Path(exists=True, file_okay=True, dir_okay=True, readable=True))
@click.argument('output_path', type=click.types.Path(file_okay=True, readable=True, writable=True))
@command_summary
def schema(target, tables, no_keys, source_path, output_path):
    """\b
    Convert XSD content into target platform schema definitions.
    Get latest schema at https://fias.nalog.ru/docs/gar_schemas.zip
    Generate file per table if `output_path` argument is existing directory;
    else dumps all tables into single file.
    """
    converter = SchemaConverterRegistry.init_converter(target)
    with ExitStack() as stack:
        storage = stack.enter_context(resolve_storage(source_path))
        output = converter.process(storage, tables, not no_keys)
    if os.path.isdir(output_path):
        for key, value in output.items():
            f = open(os.path.join(output_path, f'{key}.{converter.get_extension()}'), "w", encoding="utf-8")
            f.write(Core.compose_copyright())
            f.write(value)
            f.close()
    else:
        f = open(output_path, "w", encoding="utf-8")
        f.write(Core.compose_copyright())
        f.write(''.join(output.values()))
        f.close()


@click.command()
@click.option('--target', type=click.Choice(DumpConverterRegistry.get_available_platforms_list()),
              default='psql', help='Target dump format')
@click.option('-r', '--region', 'regions', type=str, multiple=True,
              default=[], help='Limit region list to process')
@click.option('-t', '--table', 'tables', type=str, multiple=True,
              default=Core.get_known_tables(), help='Limit table list to process')
@click.option('-m', '--mode', type=click.Choice(OutputRegistry.get_available_modes_list()),
              default='region_tree', help='Dump output mode (only if `output_path` argument is a valid directory)')
@click.argument('source_path', type=click.types.Path(exists=True, file_okay=True, dir_okay=True, readable=True))
@click.argument('output_path', type=click.types.Path(file_okay=True, readable=True, writable=True))
@click.argument('schema_path', type=click.types.Path(exists=True, file_okay=True, dir_okay=True, readable=True), required=False)
@command_summary
def dump(target, regions, tables, mode, source_path, output_path, schema_path):
    """\b
    Convert XML content into target platform dump files.
    Get latest data at https://fias.nalog.ru/Frontend
    """
    if schema_path is None:
        schema_path = source_path

    regions = list(regions)

    if not os.path.isdir(output_path):
        mode = 'direct'

    include_meta = True
    if target in ['csv', 'tsv']:
        include_meta = False
        if mode != 'region_tree':
            raise UnknownPlatformError("Cant mix multiple tables in single file")

    with ExitStack() as stack:
        source_storage = stack.enter_context(resolve_storage(source_path))
        if schema_path == source_path:
            schema_storage = source_storage
        else:
            schema_storage = stack.enter_context(resolve_storage(schema_path))

        if len(regions) == 0:
            regions = source_storage.list_regions()

        converter = DumpConverterRegistry.init_converter(target, source_storage, schema_storage)
        output = OutputRegistry.init_output(mode, converter, output_path, include_meta)
        output.write(tables, regions)

@cli.command()
@click.option('-r', '--region', 'regions', type=str, multiple=True,
              default=[], help='Limit region list to process')
@click.option('-t', '--table', 'tables', type=str, multiple=True,
              default=Core.get_known_tables(), help='Limit table list to process')
@click.option('--jobs', type=int, default=None, help='Number of worker processes to use')
@click.option('--dsn', type=str, default=None, help='PostgreSQL DSN string')
@click.option('--host', type=str, default=None, help='Database host')
@click.option('--port', type=int, default=None, help='Database port')
@click.option('--user', type=str, default=None, help='Database user')
@click.option('--password', type=str, default=None, help='Database password')
@click.option('--database', type=str, default=None, help='Database name')
@click.option('--keep-zip', is_flag=True, help='Keep downloaded archive after completion')
@click.argument('source', type=str)
@command_summary
def pipeline(regions, tables, jobs, dsn, host, port, user, password, database, keep_zip, source):
    """\b
    Run full pipeline: download archive, prepare dumps and import them into PostgreSQL.
    """
    tables = list(tables) if tables else list(Core.get_known_tables())
    regions = list(regions)
    db_config = DatabaseConfig(
        dsn=dsn,
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    if db_config.dsn is None and db_config.database is None:
        raise click.UsageError('Either --dsn or --database must be provided')
    options = PipelineOptions(
        source=source,
        tables=tables,
        regions=regions,
        jobs=jobs,
        keep_zip=keep_zip,
    )
    execute_pipeline(options, db_config)

cli.add_command(schema)
cli.add_command(dump)
cli.add_command(pipeline)

if __name__ == '__main__':
    cli()
