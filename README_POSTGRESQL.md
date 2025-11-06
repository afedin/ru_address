# PostgreSQL импорт данных ГАР

Пакет `ru_address` включает команду `pipeline`, которая автоматизирует полный цикл:

1. загрузка ZIP архива с данными (HTTP/HTTPS/FTP или локальный путь);
2. скачивание XSD схемы (по умолчанию с https://fias.nalog.ru/docs/gar_schemas.zip, либо из источника, указанного флагом `--schema`);
3. генерация дампов таблиц напрямую из архивов без распаковки;
4. параллельный импорт данных в PostgreSQL через `psql`.

## Требования

- установленный `psql` (PostgreSQL 12+);
- права на создание/изменение таблиц в целевой базе;
- достаточно свободного места для временных файлов (создаются в системной `TemporaryDirectory`).

## Быстрый старт

```shell
$ ru_address pipeline \
    --dsn postgresql://user:password@localhost/gar \
    --jobs 4 \
    https://example.org/gar.zip
```

Если предпочтительнее передавать параметры подключения отдельно:

```shell
$ ru_address pipeline \
    --host localhost \
    --port 5432 \
    --user gar_user \
    --password example \
    --database gar \
    /path/to/gar.zip
```

* `--table` позволяет ограничить список таблиц (по умолчанию загружаются все известные);
* `--region` фильтрует регионы (по умолчанию забираются все каталоги из архива);
* `--jobs` задаёт количество воркеров `ProcessPoolExecutor`;
* `--keep-zip` оставляет скачанный архив данных в рабочей директории;
* `--schema` позволяет указать локальный путь или URL для XSD вместо дефолтной ссылки.

В случае ошибки импорта соответствующий SQL-файл сохраняется рядом с текущим каталогом под именем `<REGION>_failed.sql` для диагностики.

## Проверка загруженных данных

После импорта данных можно проверить их целостность и получить статистику с помощью команды `verify`:

```shell
$ ru_address verify --dsn postgresql://user:password@localhost/gar
```

Команда выводит:
- Общее количество объектов адресации (адреса, дома, квартиры, участки, комнаты)
- Количество основных объектов (addr_obj + houses + steads)
- Статистику по таблице normative_docs (включая записи с NULL в поле name)
- Список всех таблиц с количеством записей и размером

### Проверка ожидаемого количества объектов

Можно задать ожидаемое количество основных объектов адресации для валидации:

```shell
$ ru_address verify \
    --dsn postgresql://user:password@localhost/gar \
    --expect 35000 \
    --tolerance 0.1
```

Параметры:
- `--expect` — ожидаемое количество основных объектов (addr_obj + houses + steads)
- `--tolerance` — допустимое отклонение в долях (по умолчанию 0.1 = 10%)

Команда вернёт ошибку (exit code 1), если количество объектов отличается от ожидаемого более чем на указанный процент.

### Пример вывода

```
=== Address Objects Summary ===
Total address objects: 54,408
  - Address objects (streets, etc): 508
  - Houses: 25,738
  - Apartments: 18,381
  - Steads (land plots): 9,329
  - Rooms: 452

Main objects (addr_obj + houses + steads): 35,575

=== Normative Documents ===
Total: 8,018
With NULL name: 345 (4.3%)

=== All Tables Statistics ===
Table                             Rows       Size
--------------------------------------------------
houses_params                  151,813      18 MB
change_history                 122,789      15 MB
steads_params                   86,886      10 MB
...
```
