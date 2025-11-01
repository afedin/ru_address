# Repository Guidelines

## Project Structure & Module Organization
- `ru_address/` is the installable package; `command.py` exposes the Click CLI, while `schema.py`, `dump.py`, `index.py`, and `output.py` handle schema conversion, data export, and formatting helpers.
- Supporting logic lives in `core.py` (GАR metadata), `common.py` (shared utilities), and `errors.py` (custom exceptions).
- XML streaming and parsing code resides in `source/xml.py`; reusable XSL templates and metadata assets sit under `resources/`.
- The project currently has no committed test suite; add new test modules under `tests/` when extending functionality.

## Build, Test, and Development Commands
- `python -m pip install -e .` installs the package in editable mode with CLI entry points.
- `ru_address schema <xsd_dir> <output_path> --target=mysql` generates SQL schema files for the selected backend.
- `ru_address dump <xml_dir> <output_path> <xsd_path> --target=psql` streams XML payloads into SQL or delimited dumps.
- `python -m ru_address.command --help` or `ru_address --help` lists available subcommands and options; use these to confirm new flags or behaviours.

## Coding Style & Naming Conventions
- Follow the existing PEP 8-compliant style: 4-space indentation, snake_case functions, and UPPER_SNAKE_CASE constants for table aliases.
- Keep modules cohesive; new CLI commands belong in `command.py`, while reusable logic should live in `common.py` or a dedicated helper module.
- Type hints are optional but welcome; include concise docstrings for public functions and classes when behaviour is non-obvious.

## Testing Guidelines
- Prefer lightweight integration tests that execute CLI commands against trimmed XML/XSD fixtures; capture expected outputs in temp directories.
- Name test files `test_<feature>.py` and place them under a new `tests/` package; run them with `python -m pytest` once added.
- Document any large fixture sources in the test module docstring so contributors understand provenance.

## Commit & Pull Request Guidelines
- Keep commit subjects short (≈50 chars) and action-focused, mirroring the existing history (`Update version`, `Fix heading`).
- Squash unrelated changes; include rationale and usage notes in the body when introducing new flags or breaking changes.
- Pull requests should describe the scenario addressed, list verification commands, and link to supporting issues or sample data when applicable.
- Add screenshots or CLI excerpts if the change affects user-facing output or log formatting.

## Data & Configuration Tips
- Document environment variables such as `RA_INCLUDE_DROP`, `RA_TABLE_ENGINE`, `RA_BATCH_SIZE`, and `RA_SQL_ENCODING` in PRs when default values change.
- When adding new backends or modes, update `resources/templates/` and provide migration notes for downstream database deployments.
