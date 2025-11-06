"""Module for verifying GAR database data integrity and statistics."""
import subprocess
from dataclasses import dataclass
from typing import Optional

from ru_address.common import Common
from ru_address.pipeline import DatabaseConfig


@dataclass
class TableStats:
    """Statistics for a single table."""
    name: str
    row_count: int
    size: str
    actual_count: Optional[int] = None
    not_actual_count: Optional[int] = None


@dataclass
class VerificationReport:
    """Report of database verification."""
    total_address_objects: int
    addr_obj_count: int
    houses_count: int
    apartments_count: int
    steads_count: int
    rooms_count: int
    table_stats: list[TableStats]
    normative_docs_null_count: int = 0
    normative_docs_total: int = 0


def _build_psql_connection_args(config: DatabaseConfig) -> tuple[list[str], dict[str, str]]:
    """Build psql connection arguments and environment."""
    command = ['psql', '--quiet', '--no-align', '--tuples-only']
    env = {}
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


def _execute_query(config: DatabaseConfig, query: str) -> str:
    """Execute a SQL query and return the result."""
    command, env_vars = _build_psql_connection_args(config)
    command.extend(['--command', query])

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        env={**subprocess.os.environ.copy(), **env_vars}
    )

    if result.returncode != 0:
        raise RuntimeError(f'Query failed: {result.stderr}')

    return result.stdout.strip()


def _get_table_stats(config: DatabaseConfig) -> list[TableStats]:
    """Get statistics for all tables."""
    query = """
    SELECT
        tablename,
        (xpath('//row/c/text()', xml_count))[1]::text::int as row_count,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
    FROM (
        SELECT
            schemaname,
            tablename,
            query_to_xml(format('SELECT COUNT(*) AS c FROM %I', tablename), false, true, '') as xml_count
        FROM pg_tables
        WHERE schemaname = 'public'
    ) t
    ORDER BY row_count DESC;
    """

    result = _execute_query(config, query)
    stats = []

    for line in result.split('\n'):
        if line.strip():
            parts = line.split('|')
            if len(parts) == 3:
                stats.append(TableStats(
                    name=parts[0].strip(),
                    row_count=int(parts[1].strip()),
                    size=parts[2].strip()
                ))

    return stats


def _get_address_object_counts(config: DatabaseConfig) -> dict[str, int]:
    """Get counts of main address objects."""
    query = """
    SELECT
        'addr_obj' as table_name, COUNT(*) as count FROM addr_obj
    UNION ALL
    SELECT 'houses', COUNT(*) FROM houses
    UNION ALL
    SELECT 'apartments', COUNT(*) FROM apartments
    UNION ALL
    SELECT 'steads', COUNT(*) FROM steads
    UNION ALL
    SELECT 'rooms', COUNT(*) FROM rooms;
    """

    result = _execute_query(config, query)
    counts = {}

    for line in result.split('\n'):
        if line.strip():
            parts = line.split('|')
            if len(parts) == 2:
                counts[parts[0].strip()] = int(parts[1].strip())

    return counts


def _get_normative_docs_stats(config: DatabaseConfig) -> tuple[int, int]:
    """Get normative_docs statistics (total, null count)."""
    query = """
    SELECT
        COUNT(*) as total,
        COUNT(*) - COUNT(name) as null_count
    FROM normative_docs;
    """

    result = _execute_query(config, query)
    if result:
        parts = result.split('|')
        if len(parts) == 2:
            return int(parts[0].strip()), int(parts[1].strip())

    return 0, 0


def verify_database(config: DatabaseConfig, show_details: bool = True) -> VerificationReport:
    """
    Verify GAR database and return statistics report.

    Args:
        config: Database configuration
        show_details: Whether to print detailed statistics to console

    Returns:
        VerificationReport with all statistics
    """
    Common.cli_output('Verifying GAR database...')

    # Get table statistics
    table_stats = _get_table_stats(config)

    # Get address object counts
    obj_counts = _get_address_object_counts(config)

    # Get normative_docs stats
    norm_total, norm_null = _get_normative_docs_stats(config)

    # Calculate totals
    addr_obj = obj_counts.get('addr_obj', 0)
    houses = obj_counts.get('houses', 0)
    apartments = obj_counts.get('apartments', 0)
    steads = obj_counts.get('steads', 0)
    rooms = obj_counts.get('rooms', 0)
    total_objects = addr_obj + houses + apartments + steads + rooms

    report = VerificationReport(
        total_address_objects=total_objects,
        addr_obj_count=addr_obj,
        houses_count=houses,
        apartments_count=apartments,
        steads_count=steads,
        rooms_count=rooms,
        table_stats=table_stats,
        normative_docs_total=norm_total,
        normative_docs_null_count=norm_null
    )

    if show_details:
        _print_report(report)

    return report


def _print_report(report: VerificationReport):
    """Print verification report to console."""
    Common.cli_output('\n=== Address Objects Summary ===')
    Common.cli_output(f'Total address objects: {report.total_address_objects:,}')
    Common.cli_output(f'  - Address objects (streets, etc): {report.addr_obj_count:,}')
    Common.cli_output(f'  - Houses: {report.houses_count:,}')
    Common.cli_output(f'  - Apartments: {report.apartments_count:,}')
    Common.cli_output(f'  - Steads (land plots): {report.steads_count:,}')
    Common.cli_output(f'  - Rooms: {report.rooms_count:,}')

    # Main objects (addr_obj + houses + steads)
    main_objects = report.addr_obj_count + report.houses_count + report.steads_count
    Common.cli_output(f'\nMain objects (addr_obj + houses + steads): {main_objects:,}')

    # Normative docs info
    if report.normative_docs_total > 0:
        null_pct = 100.0 * report.normative_docs_null_count / report.normative_docs_total
        Common.cli_output(f'\n=== Normative Documents ===')
        Common.cli_output(f'Total: {report.normative_docs_total:,}')
        Common.cli_output(f'With NULL name: {report.normative_docs_null_count:,} ({null_pct:.1f}%)')

    # Table statistics
    Common.cli_output('\n=== All Tables Statistics ===')
    Common.cli_output(f'{"Table":<25} {"Rows":>12} {"Size":>10}')
    Common.cli_output('-' * 50)

    for stat in report.table_stats[:20]:  # Show top 20 tables
        Common.cli_output(f'{stat.name:<25} {stat.row_count:>12,} {stat.size:>10}')


def check_expected_counts(config: DatabaseConfig, expected_main_objects: int, tolerance: float = 0.1):
    """
    Check if loaded data matches expected counts.

    Args:
        config: Database configuration
        expected_main_objects: Expected number of main objects (addr_obj + houses + steads)
        tolerance: Acceptable deviation percentage (default 10%)

    Raises:
        ValueError: If counts are outside tolerance range
    """
    report = verify_database(config, show_details=False)
    main_objects = report.addr_obj_count + report.houses_count + report.steads_count

    diff = abs(main_objects - expected_main_objects)
    diff_pct = 100.0 * diff / expected_main_objects

    if diff_pct > tolerance * 100:
        raise ValueError(
            f'Data verification failed: expected ~{expected_main_objects:,} main objects, '
            f'got {main_objects:,} ({diff_pct:.1f}% difference)'
        )

    Common.cli_output(f'✓ Data verification passed: {main_objects:,} objects loaded '
                     f'(expected ~{expected_main_objects:,})')
