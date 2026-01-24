#!/usr/bin/env python3
import argparse
import logging
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2 import sql

# DB config (hardcoded)
DBCFG = dict(
    dbname="cpibd",
    user="postgres",
    password="gfhjkm_1",
    host="192.168.2.16",
    port="5432",
    options="-c statement_timeout=6000000",
)
CHUNK_SIZE = 100


INSERT_SQL = sql.SQL(
    """
    INSERT INTO {target_table} (
        house_objectid,
        region,
        administrative_district,
        municipal_district,
        settlement,
        city,
        locality,
        planning,
        street,
        land_plot,
        house,
        full_address
    )
    WITH target AS (
        SELECT
            h.objectid AS target_objectid,
            h.housenum,
            h.addnum1,
            h.addnum2,
            h.housetype,
            mh.path
        FROM houses h
        JOIN mun_hierarchy mh
            ON mh.objectid = h.objectid
           AND mh.isactive = 1
        WHERE h.isactual = 1
          AND h.objectid = ANY(%s)
    ),
    path_elements AS (
        SELECT
            t.target_objectid,
            t.housenum,
            t.addnum1,
            t.addnum2,
            t.housetype,
            unnest(string_to_array(t.path, '.'))::bigint AS element_objectid,
            generate_subscripts(string_to_array(t.path, '.'), 1) AS element_position
        FROM target t
    ),
    addr_components AS (
        SELECT
            pe.*,
            a.level::int AS level,
            concat_ws(' ', NULLIF(a.typename, ''), a.name) AS label
        FROM path_elements pe
        JOIN addr_obj a
            ON a.objectid = pe.element_objectid
           AND a.isactual = 1
    )
    SELECT
        target_objectid AS house_objectid,
        MAX(CASE WHEN level = 1 THEN label END) AS region,
        MAX(CASE WHEN level = 2 THEN label END) AS administrative_district,
        MAX(CASE WHEN level = 3 THEN label END) AS municipal_district,
        MAX(CASE WHEN level = 4 THEN label END) AS settlement,
        MAX(CASE WHEN level = 5 THEN label END) AS city,
        MAX(CASE WHEN level = 6 THEN label END) AS locality,
        MAX(CASE WHEN level = 7 THEN label END) AS planning,
        MAX(CASE WHEN level = 8 THEN label END) AS street,
        MAX(CASE WHEN level = 9 THEN label END) AS land_plot,
        '\u0434. ' || housenum
            || CASE WHEN addnum1 IS NOT NULL THEN ' \u043a\u043e\u0440\u043f. ' || addnum1 ELSE '' END
            || CASE WHEN addnum2 IS NOT NULL THEN ' \u0441\u0442\u0440. ' || addnum2 ELSE '' END
            AS house,
        string_agg(label, ', ' ORDER BY element_position)
            || ', \u0434. ' || housenum
            || CASE WHEN addnum1 IS NOT NULL THEN ' \u043a\u043e\u0440\u043f. ' || addnum1 ELSE '' END
            || CASE WHEN addnum2 IS NOT NULL THEN ' \u0441\u0442\u0440. ' || addnum2 ELSE '' END
            AS full_address
    FROM addr_components
    GROUP BY target_objectid, housenum, addnum1, addnum2, housetype
    ON CONFLICT (house_objectid) DO UPDATE SET
        region = EXCLUDED.region,
        administrative_district = EXCLUDED.administrative_district,
        municipal_district = EXCLUDED.municipal_district,
        settlement = EXCLUDED.settlement,
        city = EXCLUDED.city,
        locality = EXCLUDED.locality,
        planning = EXCLUDED.planning,
        street = EXCLUDED.street,
        land_plot = EXCLUDED.land_plot,
        house = EXCLUDED.house,
        full_address = EXCLUDED.full_address
    """
)


CREATE_TABLE_SQL = sql.SQL(
    """
    CREATE TABLE IF NOT EXISTS {target_table} (
        house_objectid bigint PRIMARY KEY,
        region text,
        administrative_district text,
        municipal_district text,
        settlement text,
        city text,
        locality text,
        planning text,
        street text,
        land_plot text,
        house text,
        full_address text
    )
    """
)


def parse_table_name(raw_value: str) -> sql.Composed:
    parts = [part for part in raw_value.split(".") if part]
    if not parts or len(parts) > 2:
        raise ValueError("table name must be 'table' or 'schema.table'")
    if len(parts) == 2:
        return sql.Identifier(parts[0], parts[1])
    return sql.Identifier(parts[0])


def fetch_chunk_ids(cursor, last_id: int, chunk_size: int) -> list[int]:
    cursor.execute(
        """
        SELECT objectid
        FROM houses
        WHERE isactual = 1
          AND objectid > %s
        ORDER BY objectid
        LIMIT %s
        """,
        (last_id, chunk_size),
    )
    return [row[0] for row in cursor.fetchall()]


def ensure_table(cursor, table_ident: sql.Composed, recreate: bool) -> None:
    if recreate:
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table_ident))
    cursor.execute(CREATE_TABLE_SQL.format(target_table=table_ident))


def get_resume_id(cursor, table_ident: sql.Composed) -> int:
    cursor.execute(sql.SQL("SELECT MAX(house_objectid) FROM {}").format(table_ident))
    value = cursor.fetchone()[0]
    return int(value) if value is not None else 0


def setup_logging(log_file: str) -> logging.Logger:
    logger = logging.getLogger("build_house_addresses")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        log_path = Path(log_file)
        if log_path.parent and not log_path.parent.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def run(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Chunked builder for a flat house address table based on mun_hierarchy."
        )
    )
    parser.add_argument(
        "--table",
        default="house_address_flat",
        help="Target table name (optionally schema.table).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from max house_objectid in target table.",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop and recreate target table before processing.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/build_house_addresses.log",
        help="Log file path (default: logs/build_house_addresses.log).",
    )
    args = parser.parse_args(argv)

    logger = setup_logging(args.log_file)

    try:
        table_ident = parse_table_name(args.table)
    except ValueError as exc:
        logger.error("Invalid --table value: %s", exc)
        return 2

    logger.info(
        "start table=%s resume=%s recreate=%s chunk_size=%s",
        args.table,
        args.resume,
        args.recreate,
        CHUNK_SIZE,
    )

    conn = psycopg2.connect(**DBCFG)
    conn.set_client_encoding("UTF8")
    conn.autocommit = False

    total_rows = 0
    chunk_idx = 0

    try:
        with conn.cursor() as cursor:
            ensure_table(cursor, table_ident, args.recreate)
            conn.commit()

            last_id = 0
            if args.resume and not args.recreate:
                last_id = get_resume_id(cursor, table_ident)

            while True:
                ids = fetch_chunk_ids(cursor, last_id, CHUNK_SIZE)
                if not ids:
                    break

                chunk_idx += 1
                last_id = ids[-1]

                start_ts = time.time()
                cursor.execute(INSERT_SQL.format(target_table=table_ident), (ids,))
                conn.commit()

                inserted = cursor.rowcount
                if inserted > 0:
                    total_rows += inserted

                elapsed = time.time() - start_ts
                logger.info(
                    "chunk=%s rows=%s last_id=%s elapsed=%.1fs",
                    chunk_idx,
                    inserted,
                    last_id,
                    elapsed,
                )
    finally:
        conn.close()

    logger.info("done: chunks=%s rows=%s", chunk_idx, total_rows)
    return 0


def main() -> None:
    raise SystemExit(run(sys.argv[1:]))


if __name__ == "__main__":
    main()
