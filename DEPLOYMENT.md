# Инструкция по развёртыванию и загрузке данных

Внимание: не используйте реальные пароли в командах и документации. Рекомендуется применять переменные окружения, `.pgpass`, или шаблонные плейсхолдеры вида `postgresql://<user>:<password>@<host>/<db>`.

## 1. Отправка изменений на сервер

```bash
# На локальной машине (Mac)
cd ~/Documents/gar/ru_address

# Синхронизация кода на сервер
rsync -av --exclude='*.pyc' --exclude='__pycache__' --exclude='.git' \
    ru_address/ user@server.example.com:~/ru_address/ru_address/

# Синхронизация документации
rsync -av README_POSTGRESQL.md DEPLOYMENT.md \
    user@server.example.com:~/ru_address/
```

## 2. Установка обновлённого пакета на сервере

```bash
# Подключаемся к серверу
ssh user@server.example.com

# Переходим в директорию проекта
cd ~/ru_address

# Активируем виртуальное окружение
source venv/bin/activate

# Устанавливаем пакет
pip install .

# Проверяем, что команда verify доступна
ru_address --help
```

## 3. Загрузка данных

### ❗ Важно: Pipeline НЕ удаляет существующие данные автоматически

Pipeline работает следующим образом:
- При первом запуске создаёт таблицы (с `DROP TABLE IF EXISTS`)
- При повторном запуске **добавляет данные** в существующие таблицы
- **НЕ очищает** данные перед импортом

### Вариант A: Добавить новый регион к существующим данным

Если хотите загрузить дополнительный регион (например, регион 77 к существующему региону 83):

```bash
cd ~/ru_address
source venv/bin/activate

# Загрузка региона 77 (Москва) В ДОПОЛНЕНИЕ к существующим данным
ru_address pipeline \
    --dsn postgresql://<user>:<password>@<host>/<db> \
    --jobs 4 \
    --region 77 \
    --schema ~/ru_address/gar_schemas.zip \
    /srv/data/gar/gar_xml.zip
```

### Вариант B: Полностью пересоздать базу с новым регионом

Если хотите удалить все данные и загрузить только новый регион:

```bash
cd ~/ru_address
source venv/bin/activate

# Шаг 1: Удаляем все таблицы
psql postgresql://<user>:<password>@<host>/<db> << 'SQL'
DROP TABLE IF EXISTS addr_obj CASCADE;
DROP TABLE IF EXISTS addr_obj_division CASCADE;
DROP TABLE IF EXISTS addr_obj_params CASCADE;
DROP TABLE IF EXISTS addr_obj_types CASCADE;
DROP TABLE IF EXISTS adm_hierarchy CASCADE;
DROP TABLE IF EXISTS apartment_types CASCADE;
DROP TABLE IF EXISTS apartments CASCADE;
DROP TABLE IF EXISTS apartments_params CASCADE;
DROP TABLE IF EXISTS carplaces CASCADE;
DROP TABLE IF EXISTS carplaces_params CASCADE;
DROP TABLE IF EXISTS change_history CASCADE;
DROP TABLE IF EXISTS house_types CASCADE;
DROP TABLE IF EXISTS houses CASCADE;
DROP TABLE IF EXISTS houses_params CASCADE;
DROP TABLE IF EXISTS mun_hierarchy CASCADE;
DROP TABLE IF EXISTS normative_docs CASCADE;
DROP TABLE IF EXISTS normative_docs_kinds CASCADE;
DROP TABLE IF EXISTS normative_docs_types CASCADE;
DROP TABLE IF EXISTS object_levels CASCADE;
DROP TABLE IF EXISTS operation_types CASCADE;
DROP TABLE IF EXISTS param_types CASCADE;
DROP TABLE IF EXISTS reestr_objects CASCADE;
DROP TABLE IF EXISTS room_types CASCADE;
DROP TABLE IF EXISTS rooms CASCADE;
DROP TABLE IF EXISTS rooms_params CASCADE;
DROP TABLE IF EXISTS steads CASCADE;
DROP TABLE IF EXISTS steads_params CASCADE;
DROP TABLE IF EXISTS addhouse_types CASCADE;
SQL

# Шаг 2: Генерируем схемы заново
ru_address schema \
    ~/ru_address/gar_schemas.zip \
    ~/schema.sql \
    --target=psql

# Шаг 3: Применяем схемы
psql postgresql://<user>:<password>@<host>/<db> -f ~/schema.sql

# Шаг 4: Загружаем данные нового региона
ru_address pipeline \
    --dsn postgresql://<user>:<password>@<host>/<db> \
    --jobs 4 \
    --region 77 \
    --schema ~/ru_address/gar_schemas.zip \
    /srv/data/gar/gar_xml.zip
```
5. Загрузка конкретных таблиц региона
 ru_address pipeline \
      --dsn postgresql://<user>:<password>@<host>/<db> \
      --jobs 4 \
      --region 77 \
      --table HOUSES \
      --table MUN_HIERARCHY \
      --table ADDR_OBJ \
      --schema ~/ru_address/gar_schemas.zip \
      /srv/data/gar/gar_xml.zip

### Вариант C: Загрузить все регионы

```bash
# Без указания --region загружаются ВСЕ регионы из архива
ru_address pipeline \
    --dsn postgresql://<user>:<password>@<host>/<db> \
    --jobs 4 \
    --schema ~/ru_address/gar_schemas.zip \
    /srv/data/gar/gar_xml.zip
```

## 4. Проверка загруженных данных

После загрузки всегда проверяйте результат:

```bash
# Базовая проверка
ru_address verify --dsn postgresql://<user>:<password>@<host>/<db>

# Проверка с ожидаемым количеством объектов
# (для региона 83 ожидается ~35000 основных объектов)
ru_address verify \
    --dsn postgresql://<user>:<password>@<host>/<db> \
    --expect 35000

# Для нескольких регионов количество будет больше
# Например, для двух регионов (83 + 77):
ru_address verify \
    --dsn postgresql://<user>:<password>@<host>/<db> \
    --expect 500000
```

## 5. Ожидаемое количество объектов по регионам

Примерные цифры основных объектов (addr_obj + houses + steads):

| Регион | Название | Примерное количество |
|--------|----------|---------------------|
| 77 | Москва | ~400,000 |
| 78 | Санкт-Петербург | ~200,000 |
| 83 | Ненецкий АО | ~35,000 |
| 50 | Московская область | ~600,000 |

**Важно**: Используйте `--tolerance 0.15` (15%) для больших регионов, так как данные могут меняться между обновлениями.

## 6. Коды регионов

```bash
# Посмотреть список доступных регионов в архиве
unzip -l /srv/data/gar/gar_xml.zip | grep -E "^.*\s[0-9]{2}/.*$" | awk '{print $4}' | cut -d'/' -f1 | sort -u
```

Популярные коды:
- 01 - Адыгея
- 02 - Башкортостан
- 77 - Москва
- 78 - Санкт-Петербург
- 50 - Московская область
- 23 - Краснодарский край
- 66 - Свердловская область
- 83 - Ненецкий АО

## 7. Устранение проблем

### Ошибка: таблицы не существуют

Если pipeline выдаёт ошибки `relation does not exist`:

```bash
# Пересоздайте схемы вручную
ru_address schema ~/ru_address/gar_schemas.zip ~/schema.sql --target=psql
psql postgresql://<user>:<password>@<host>/<db> -f ~/schema.sql
```

### Ошибка: NULL value in column "name"

Если видите ошибку про NOT NULL constraint:
1. Убедитесь, что используете обновлённый код с исправлением в `postgres.schema.xsl`
2. Пересоздайте схемы (см. Вариант B, Шаг 2-3)

### Проверка актуальной версии кода

```bash
# На сервере проверьте, что исправление применено
grep -A 3 "String fields are always nullable" \
    ~/ru_address/ru_address/resources/templates/postgres.schema.xsl

# Должно вывести комментарий про nullable string fields
```

## 8. Мониторинг процесса загрузки

```bash
# В отдельном терминале можно следить за процессом
watch -n 5 'psql postgresql://<user>:<password>@<host>/<db> -c "
SELECT
  tablename,
  n_live_tup as rows
FROM pg_stat_user_tables
WHERE schemaname = '\''public'\''
ORDER BY n_live_tup DESC
LIMIT 10;
"'
```

## 9. Полезные SQL запросы

```sql
-- Проверить количество данных по регионам (если есть поле region_code)
SELECT COUNT(*) FROM houses;
SELECT COUNT(*) FROM apartments;
SELECT COUNT(*) FROM addr_obj;

-- Размер базы данных
SELECT pg_size_pretty(pg_database_size('gar'));

-- Найти дубликаты (если загружали регион дважды)
SELECT objectid, COUNT(*)
FROM houses
GROUP BY objectid
HAVING COUNT(*) > 1
LIMIT 10;
```

## 10. Бэкап перед загрузкой

Рекомендуется создать бэкап перед загрузкой новых данных:

```bash
# Создать бэкап
pg_dump postgresql://<user>:<password>@<host>/<db> > ~/gar_backup_$(date +%Y%m%d).sql

# Восстановить из бэкапа
psql postgresql://<user>:<password>@<host>/<db> < ~/gar_backup_20250106.sql
```
