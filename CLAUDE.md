# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`ru_address` is a Python utility for converting the GAR (Global Address Register, formerly known as FIAS/KLADR) database schema and data dumps for import into MySQL/PostgreSQL/ClickHouse databases. The tool uses a SAX parser to consume minimal resources (~50 MB memory) when processing large XML files.

**Key Features:**
- Converts XSD schemas to database-specific DDL (MySQL, PostgreSQL, ClickHouse)
- Converts XML data dumps to SQL inserts or CSV/TSV formats
- Supports region-based filtering and multiple output modes
- Memory-efficient processing using SAX parsing via lxml

## Development Commands

### Installation
```bash
pip install .
```

### Running the Tool
The package provides the `ru_address` command with four main subcommands:

**Schema conversion:**
```bash
ru_address schema /path/to/xsd /path/to/output --target=[mysql|psql|ch]
```

**Data dump conversion:**
```bash
ru_address dump /path/to/xml /path/to/output /path/to/xsd --target=[mysql|psql|csv|tsv]
```

**Full pipeline (PostgreSQL):**
```bash
ru_address pipeline --dsn postgresql://user:pass@host/db --region 83 /path/to/gar.zip
```

**Database verification:**
```bash
ru_address verify --dsn postgresql://user:pass@host/db --expect 35000
```

### Linting
```bash
pylint ru_address
```
Pylint is configured via `.pylintrc` with max line length of 120 characters.

### Running Directly as Module
```bash
python -m ru_address.command schema [OPTIONS]
python -m ru_address.command dump [OPTIONS]
```

## Architecture

### Core Components

**command.py** (ru_address/command.py)
- Entry point defining CLI using Click framework
- Four main commands: `schema`, `dump`, `pipeline`, `verify`
- Handles environment variable injection via `-e` flag
- Tracks execution time and memory usage via `@command_summary` decorator

**core.py** (ru_address/core.py)
- Defines `KNOWN_ENTITIES`: list of all GAR table types from the XSD schema
- `COMMON_TABLE_LIST`: tables with common/reference data (address types, house types, etc.)
- `REGION_TABLE_LIST`: tables with regional data (addresses, houses, apartments, etc.)
- Provides utility methods for generating copyright headers and table separators

**schema.py** (ru_address/schema.py)
- `ConverterRegistry`: Registry pattern for schema converters (MySQL, PostgreSQL, ClickHouse)
- `BaseSchemaConverter`: Abstract base class using XSLT transformations
- Each converter uses platform-specific XSL templates from `resources/templates/`
- Supports environment variables: `RA_INCLUDE_DROP`, `RA_TABLE_ENGINE`

**dump.py** (ru_address/dump.py)
- `ConverterRegistry`: Registry for data dump converters (MySQL, PostgreSQL, CSV, TSV)
- `BaseDumpConverter`: Abstract base for converting XML data to target formats
- Each converter defines custom `TableRepresentation` with escape rules, delimiters, quote styles
- Supports environment variables: `RA_BATCH_SIZE`, `RA_SQL_ENCODING`

**output.py** (ru_address/output.py)
- `OutputRegistry`: Registry for output modes
- Four output modes:
  - `direct`: Single file with all data
  - `per_region`: One file per region
  - `per_table`: One file per table
  - `region_tree`: Mirrors source XML directory structure
- Handles writing dump headers/footers and table separators

**source/xml.py** (ru_address/source/xml.py)
- `Definition`: Parses XSD schema files to extract table structure
- `Data`: SAX-based XML parser for processing data files
- Memory-efficient streaming approach for large files

**index.py** (ru_address/index.py)
- Generates database indexes/keys using XSLT transformations
- Uses `resources/index.xml` as source definition
- Platform-specific index templates in `resources/templates/`

**pipeline.py** (ru_address/pipeline.py)
- Implements `pipeline` command for automated PostgreSQL import workflow
- Downloads data archives (HTTP/HTTPS/FTP or local paths)
- Automatically fetches XSD schemas from https://fias.nalog.ru/docs/gar_schemas.zip
- Generates SQL dumps and imports them into PostgreSQL via `psql`
- Supports parallel processing of regions using `ProcessPoolExecutor`
- Defines `DatabaseConfig` and `PipelineOptions` dataclasses

**verify.py** (ru_address/verify.py)
- Implements `verify` command for database integrity checking
- Queries PostgreSQL database to gather statistics on loaded data
- Reports total address objects, table sizes, and row counts
- Special handling for `normative_docs` table with nullable `name` field
- Supports validation against expected object counts with configurable tolerance
- Defines `TableStats` and `VerificationReport` dataclasses

### Data Flow

1. **Schema Conversion**: XSD → XSLT Transform → Platform-specific DDL
2. **Data Conversion**: XML Source → SAX Parser → Batch Writer → Target Format

### Registry Pattern

The codebase extensively uses the Registry pattern for extensibility:
- `SchemaConverterRegistry` (schema.py): Maps platform names to schema converters
- `DumpConverterRegistry` (dump.py): Maps platform names to dump converters
- `OutputRegistry` (output.py): Maps mode names to output handlers

To add new platforms/modes, create a new class inheriting from the base class and register it in the respective registry's `get_available_platforms()` or `get_available_modes()` method.

### Environment Variables

Configuration is primarily done via environment variables:
- `RA_INCLUDE_DROP`: Include DROP TABLE statements (default: "1")
- `RA_TABLE_ENGINE`: MySQL/ClickHouse table engine (default: "MyISAM"/"MergeTree")
- `RA_BATCH_SIZE`: INSERT batch size (default: "500")
- `RA_SQL_ENCODING`: MySQL character encoding (default: "utf8mb4")

Pass via: `ru_address -e VAR_NAME value command ...` or standard shell ENV variables.

### XSLT Templates

Schema and index generation relies heavily on XSLT transformations. Templates are in `resources/templates/`:
- `{platform}.schema.xsl`: Table DDL generation
- `{platform}.index.xsl`: Index/key generation

The `index.xml` file defines the minimal set of indexes for all tables.

## Important Fixes

### PostgreSQL String Fields Nullable Fix

**Issue**: The official GAR XSD schemas mark certain string fields (like `normative_docs.name`) as required (`use="required"`), but actual data from FNS contains NULL values in these fields. This caused import failures with errors like:

```
ERROR: null value in column "name" of relation "normative_docs" violates not-null constraint
```

**Solution**: Modified `resources/templates/postgres.schema.xsl` (lines 68-79) to make all string fields (`xs:string` type) nullable by default, regardless of XSD schema requirements. This prevents NOT NULL constraints on varchar/text fields, allowing the import to succeed even when source data contains unexpected NULLs.

**Affected Template Logic**:
```xsl
<xsl:choose>
  <!-- String fields are always nullable because actual GAR data contains NULLs even when schema says required -->
  <xsl:when test="xs:simpleType/xs:restriction/@base='xs:string' or @type='xs:string'">
    <xsl:text> NULL DEFAULT NULL</xsl:text>
  </xsl:when>
  <xsl:when test="@use='required'">
    <xsl:text> NOT NULL</xsl:text>
  </xsl:when>
  ...
</xsl:choose>
```

This ensures data integrity while accommodating inconsistencies in official GAR data releases.
