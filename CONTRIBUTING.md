# Руководство по разработке (CONTRIBUTING)

## Структура проекта
- Исходный код: `ru_address/` (CLI — `command.py`, конвертеры — `dump.py`, хранилища — `storage.py`, пайплайн — `pipeline.py`).
- Ресурсы XSLT/схемы: `ru_address/resources/`.
- Вспомогательные SQL: `ADDRESS_QUERIES.sql`, `MOSCOW_MUNICIPAL_QUERIES.sql` (рядом с пакетом).
- Тесты: `tests/` (минимальные фикстуры создаются на лету во временных каталогах/архивах).

## Установка и запуск
- Локальная установка: `python -m pip install -e .[dev]` (или `pip install .`).
- CLI:
  - `ru_address schema <schema_dir_or_zip> <out> [--target=psql|mysql|ch]`
  - `ru_address dump <data_dir_or_zip> <out_path_or_dir> <schema_dir_or_zip> [--target=psql|csv|tsv]`
  - `ru_address pipeline --dsn postgresql://<user>:<password>@<host>/<db> [--jobs N] [--region 77] <gar.zip>`
  - `ru_address verify --dsn postgresql://<user>:<password>@<host>/<db> [--expect N --tolerance 0.1]`

Примечание: поддерживается чтение данных и схем напрямую из ZIP‑архивов (без распаковки), а также из распакованных каталогов.

## Разработка и тестирование
- Запуск тестов: `python -m pytest -q` или `pytest tests/test_pipeline.py::PipelineRegionTest -k process_region`.
- Требования для части интеграционных тестов: Python ≥ 3.10, `lxml`, доступность `psql` (для пайплайна и верификации).

## Стиль кода и именование
- Python 3.10+: отступы 4 пробела, имена модулей — в нижнем регистре, классы — PascalCase, функции — snake_case.
- Опции CLI — в kebab-case.
- Лимит длины строки — 120 символов (`.pylintrc`).
- Потоковая обработка XML: не загружать дерево целиком, использовать SAX/iterparse.

## Переменные окружения
- `RA_BATCH_SIZE` — размер пакета для INSERT (по умолчанию 500).
- `RA_SQL_ENCODING` — кодировка для MySQL (по умолчанию `utf8mb4`).
- `RA_INCLUDE_DROP` — добавлять `DROP IF EXISTS` в DDL (по умолчанию `1`).
- `RA_TABLE_ENGINE` — движок таблиц для MySQL/ClickHouse (например, `MyISAM`/`MergeTree`).

## Проверки и пайплайн
- Пайплайн не очищает таблицы автоматически при повторных запусках — данные добавляются. Для полной перезагрузки предварительно удаляйте таблицы.
- Не публикуйте реальные пароли/DSN в README/DEPLOYMENT — используйте плейсхолдеры или `.pgpass`.

## Коммиты и pull request’ы
- Пишите предметные заголовки коммитов в стиле: «Добавить verify для проверки целостности…».
- В PR указывайте мотивацию/изменения CLI/API/новые переменные окружения и пример команд. Прикладывайте результаты тестов.

