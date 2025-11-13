-- ============================================================================
-- SQL запросы для работы с муниципальным делением Москвы
-- ============================================================================
-- Примерные запросы рассчитаны на PostgreSQL; конкретная среда исполнения не принципиальна
-- Москва: objectid = 1405113, objectguid = 0c5b2444-70a0-4932-980c-b4dc0d3f02b5
--
-- ВАЖНО: Москва является корневым элементом (уровень 1 - Субъект РФ)
-- и НЕ ИМЕЕТ ПРЕДКОВ в иерархии (parentobjid = NULL)
-- ============================================================================

-- ============================================================================
-- 1. ИНФОРМАЦИЯ О МОСКВЕ
-- ============================================================================

SELECT
    a.objectid,
    a.objectguid,
    a.name,
    a.typename,
    a.level,
    ol.name as level_name,
    mh.path,
    mh.parentobjid
FROM addr_obj a
JOIN mun_hierarchy mh ON a.objectid = mh.objectid
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE a.objectid = 1405113
  AND a.isactual = 1
  AND mh.isactive = 1;

-- Результат:
--  objectid |              objectguid              |  name  | typename | level | level_name |  path   | parentobjid
-- ----------+--------------------------------------+--------+----------+-------+------------+---------+-------------
--   1405113 | 0c5b2444-70a0-4932-980c-b4dc0d3f02b5 | Москва | г        | 1     | Субъект РФ | 1405113 |


-- ============================================================================
-- 2. ПРЕДКИ МОСКВЫ (путь в иерархии)
-- ============================================================================
-- Москва не имеет предков, так как является корневым элементом (Субъект РФ)

WITH path_elements AS (
    SELECT
        h.objectid as target_objectid,
        h.path,
        unnest(string_to_array(h.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(h.path, '.'), 1) as element_position
    FROM mun_hierarchy h
    WHERE h.objectid = 1405113
      AND h.isactive = 1
)
SELECT
    pe.element_position as position,
    pe.element_objectid as objectid,
    a.level,
    ol.name as level_name,
    a.typename,
    a.name,
    CASE
        WHEN a.typename IS NOT NULL AND a.typename != ''
        THEN a.typename || ' ' || a.name
        ELSE a.name
    END as display_name
FROM path_elements pe
JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
ORDER BY pe.element_position;

-- Результат:
--  position | objectid | level | level_name | typename |  name  | display_name
-- ----------+----------+-------+------------+----------+--------+--------------
--         1 |  1405113 | 1     | Субъект РФ | г        | Москва | г Москва


-- ============================================================================
-- 3. СТАТИСТИКА МУНИЦИПАЛЬНОГО ДЕЛЕНИЯ МОСКВЫ
-- ============================================================================
-- Показывает количество объектов по уровням (только муниципальное деление, без улиц)

SELECT
    a.level::int as level_num,
    ol.name as level_name,
    COUNT(*) as object_count
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE mh.path LIKE '1405113%'
  AND mh.isactive = 1
  AND a.level::int <= 6  -- Только муниципальное деление (исключаем улицы и планировку)
GROUP BY a.level::int, ol.name
ORDER BY a.level::int;

-- Результат:
--  level_num |     level_name      | object_count
-- -----------+---------------------+--------------
--          1 | Субъект РФ          |            1
--          3 | Муниципальный район |          136
--          5 | Город               |            8
--          6 | Населенный пункт    |          341


-- ============================================================================
-- 4. СПИСОК ВСЕХ МУНИЦИПАЛЬНЫХ ОКРУГОВ МОСКВЫ (уровень 3)
-- ============================================================================

SELECT
    ROW_NUMBER() OVER (ORDER BY a.name) as num,
    a.objectid,
    a.name,
    a.typename,
    mh.path
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
WHERE mh.parentobjid = 1405113
  AND mh.isactive = 1
  AND a.level = '3'
ORDER BY a.name;

-- Всего: 136 муниципальных округов и поселений
-- Примеры:
--  1 | 95251365 | городской округ Троицк            | вн.тер.г. | 1405113.95251365
--  2 | 95251310 | муниципальный округ Академический | вн.тер.г. | 1405113.95251310
--  5 | 95251254 | муниципальный округ Арбат         | вн.тер.г. | 1405113.95251254


-- ============================================================================
-- 5A. ДЕРЕВО МУНИЦИПАЛЬНОГО ДЕЛЕНИЯ ДЛЯ КОНКРЕТНОГО ОКРУГА (БЕЗ УЛИЦ)
-- ============================================================================
-- Пример: городской округ Троицк (objectid = 95251365)
-- Показывает ТОЛЬКО муниципальное деление: округ -> город -> деревни/поселки

WITH RECURSIVE hierarchy AS (
    -- Начинаем с округа Троицк
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int,
        0 as depth
    FROM mun_hierarchy h
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.objectid = 95251365  -- ПАРАМЕТР: objectid округа
      AND h.isactive = 1

    UNION ALL

    -- Получаем всех потомков
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int,
        p.depth + 1
    FROM hierarchy p
    JOIN mun_hierarchy h ON h.parentobjid = p.objectid
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.isactive = 1
      AND a.level::int <= 6  -- Только муниципальное деление (БЕЗ улиц и планировки)
)
SELECT
    REPEAT('  ', depth) || name as name_indented,
    objectid,
    level,
    typename,
    depth
FROM hierarchy
ORDER BY path;

-- Результат:
--      name_indented      | objectid | level | typename  | depth
-- ------------------------+----------+-------+-----------+-------
--  городской округ Троицк | 95251365 |     3 | вн.тер.г. |     0
--    Троицк               |   811934 |     5 | г         |     1
--    Кувекино             |   855911 |     6 | д         |     1
--    Пыхчево              |   856114 |     6 | д         |     1
--    Десна                |   857504 |     6 | д         |     1
--    ... (всего 22 населенных пункта)


-- ============================================================================
-- 5B. ПОЛНОЕ ДЕРЕВО ДЛЯ КОНКРЕТНОГО ОКРУГА (СО ВСЕМИ ОБЪЕКТАМИ)
-- ============================================================================
-- Пример: городской округ Троицк (objectid = 95251365)
-- Показывает ВСЁ: округ -> город -> деревни -> улицы -> планировка

WITH RECURSIVE hierarchy AS (
    -- Начинаем с округа Троицк
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int,
        0 as depth
    FROM mun_hierarchy h
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.objectid = 95251365  -- ПАРАМЕТР: objectid округа
      AND h.isactive = 1

    UNION ALL

    -- Получаем всех потомков БЕЗ ОГРАНИЧЕНИЙ
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int,
        p.depth + 1
    FROM hierarchy p
    JOIN mun_hierarchy h ON h.parentobjid = p.objectid
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.isactive = 1
    -- БЕЗ ОГРАНИЧЕНИЙ по level - получаем ВСЁ включая улицы и планировку
)
SELECT
    REPEAT('  ', depth) || name as name_indented,
    objectid,
    level,
    typename,
    depth
FROM hierarchy
ORDER BY path
LIMIT 100;  -- первые 100 записей для примера

-- Результат (первые записи):
--      name_indented       | objectid  | level | typename  | depth
-- -------------------------+-----------+-------+-----------+-------
--  городской округ Троицк  |  95251365 |     3 | вн.тер.г. |     0
--    Генерала Пилипенко    | 103161568 |     8 | ул        |     1
--    Гренадерская          | 105069884 |     8 | ул        |     1
--    Драгунская            | 105069885 |     8 | ул        |     1
--    ... (всего 497 объектов: 1 округ + 1 город + 20 деревень + 172 планировки + 303 улицы)


-- ============================================================================
-- 5C. ПОДСЧЕТ ОБЪЕКТОВ ПО УРОВНЯМ В КОНКРЕТНОМ ОКРУГЕ
-- ============================================================================

SELECT
    a.level::int as level_num,
    ol.name as level_name,
    COUNT(*) as count
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE mh.path LIKE '1405113.95251365%'  -- ПАРАМЕТР: path округа + %
  AND mh.isactive = 1
GROUP BY a.level::int, ol.name
ORDER BY a.level::int;

-- Результат для округа Троицк:
--  level_num |           level_name            | count
-- -----------+---------------------------------+-------
--          3 | Муниципальный район             |     1
--          5 | Город                           |     1
--          6 | Населенный пункт                |    20
--          7 | Элемент планировочной структуры |   172
--          8 | Элемент улично-дорожной сети    |   303


-- ============================================================================
-- 6. ПОЛНАЯ ИЕРАРХИЯ ДЛЯ ЛЮБОГО ОБЪЕКТА (С ПРЕДКАМИ)
-- ============================================================================
-- Универсальный запрос для получения предков любого объекта
-- Пример: улица Арбат (objectid = 1447085)

WITH path_elements AS (
    SELECT
        h.objectid as target_objectid,
        h.path,
        unnest(string_to_array(h.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(h.path, '.'), 1) as element_position
    FROM mun_hierarchy h
    WHERE h.objectid = 1447085  -- ПАРАМЕТР: objectid любого объекта
      AND h.isactive = 1
)
SELECT
    pe.element_position as position,
    pe.element_objectid as objectid,
    a.level,
    ol.name as level_name,
    a.typename,
    a.name,
    CASE
        WHEN a.typename IS NOT NULL AND a.typename != ''
        THEN a.typename || ' ' || a.name
        ELSE a.name
    END as display_name
FROM path_elements pe
JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
ORDER BY pe.element_position;

-- Результат для улицы Арбат:
--  position | objectid | level |          level_name          | typename  |           name            |            display_name
-- ----------+----------+-------+------------------------------+-----------+---------------------------+-------------------------------------
--         1 |  1405113 | 1     | Субъект РФ                   | г         | Москва                    | г Москва
--         2 | 95251254 | 3     | Муниципальный район          | вн.тер.г. | муниципальный округ Арбат | вн.тер.г. муниципальный округ Арбат
--         3 |  1447085 | 8     | Элемент улично-дорожной сети | ул        | Арбат                     | ул Арбат


-- ============================================================================
-- 7. ПОЛНЫЙ АДРЕС СТРОКОЙ
-- ============================================================================

WITH path_elements AS (
    SELECT
        h.objectid as target_objectid,
        h.path,
        unnest(string_to_array(h.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(h.path, '.'), 1) as element_position
    FROM mun_hierarchy h
    WHERE h.objectid = 1447085  -- ПАРАМЕТР: objectid объекта
      AND h.isactive = 1
)
SELECT
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY pe.element_position
    ) as full_address
FROM path_elements pe
JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1;

-- Результат:
--  full_address: г Москва, вн.тер.г. муниципальный округ Арбат, ул Арбат


-- ============================================================================
-- 8. ТОЛЬКО МУНИЦИПАЛЬНОЕ ДЕЛЕНИЕ (БЕЗ УЛИЦ И ПЛАНИРОВКИ)
-- ============================================================================
-- Получить все объекты муниципального деления в Москве (уровни 1-6)

SELECT
    a.objectid,
    a.level::int as level_num,
    ol.name as level_name,
    a.typename,
    a.name,
    mh.path,
    mh.parentobjid
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE mh.path LIKE '1405113%'
  AND mh.isactive = 1
  AND a.level::int BETWEEN 1 AND 6  -- Только муниципальное деление
ORDER BY
    a.level::int,
    mh.path;


-- ============================================================================
-- 9. НАЙТИ КОНКРЕТНЫЙ ОКРУГ ПО ИМЕНИ
-- ============================================================================

SELECT
    a.objectid,
    a.name,
    a.typename,
    a.level,
    mh.path
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
WHERE mh.parentobjid = 1405113
  AND mh.isactive = 1
  AND a.level = '3'
  AND a.name ILIKE '%Арбат%'  -- ПАРАМЕТР: часть названия (без учета регистра)
ORDER BY a.name;


-- ============================================================================
-- 10. НАСЕЛЕННЫЕ ПУНКТЫ В КОНКРЕТНОМ ОКРУГЕ
-- ============================================================================

SELECT
    a.objectid,
    a.name,
    a.typename,
    a.level,
    ol.name as level_name,
    mh.path
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE mh.path LIKE '1405113.95251365.%'  -- ПАРАМЕТР: path округа + .%
  AND mh.isactive = 1
  AND a.level::int IN (5, 6)  -- Города и населенные пункты
ORDER BY a.level::int, a.name;


-- ============================================================================
-- 11. РАБОТА С ДОМАМИ (уровень 10)
-- ============================================================================
-- ВАЖНО: Дома хранятся в отдельной таблице houses (не в addr_obj)

-- 11A. Подсчет домов в Москве
SELECT COUNT(*) as houses_count
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
WHERE mh.path LIKE '1405113%'
  AND h.isactual = 1
  AND mh.isactive = 1;

-- Результат: 293,602 дома


-- 11B. Дома на конкретной улице
SELECT
    h.objectid,
    h.housenum,
    h.addnum1,
    h.addnum2,
    h.housetype,
    CASE
        WHEN h.housetype = 1 THEN 'Дом'
        WHEN h.housetype = 2 THEN 'Домовладение'
        WHEN h.housetype = 3 THEN 'Гараж'
        WHEN h.housetype = 4 THEN 'Здание'
        WHEN h.housetype = 5 THEN 'Шахта'
        WHEN h.housetype = 6 THEN 'Строение'
        WHEN h.housetype = 7 THEN 'Сооружение'
        WHEN h.housetype = 8 THEN 'Литера'
        WHEN h.housetype = 9 THEN 'Корпус'
        WHEN h.housetype = 10 THEN 'Подвал'
        WHEN h.housetype = 11 THEN 'Котельная'
        WHEN h.housetype = 12 THEN 'Погреб'
        WHEN h.housetype = 13 THEN 'Объект незавершенного строительства'
        ELSE 'Другое'
    END as housetype_name
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
WHERE mh.parentobjid = 1447085  -- ПАРАМЕТР: objectid улицы (Арбат)
  AND h.isactual = 1
  AND mh.isactive = 1
ORDER BY h.housenum
LIMIT 20;

-- Результат для улицы Арбат:
--  objectid | housenum | addnum1 | addnum2 | housetype | housetype_name
-- ----------+----------+---------+---------+-----------+----------------
--  66941203 | 1        |         |         |         2 | Домовладение
--  67054556 | 10       |         |         |         2 | Домовладение
--  66999540 | 11       |         |         |         2 | Домовладение
--  ...


-- 11C. Подсчет домов в округе
SELECT COUNT(*) as houses_count
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
WHERE mh.path LIKE '1405113.95251365%'  -- ПАРАМЕТР: path округа + %
  AND h.isactual = 1
  AND mh.isactive = 1;

-- Результат для округа Троицк: 14,217 домов


-- 11D. Полная статистика округа ВКЛЮЧАЯ ДОМА
SELECT
    'addr_obj' as source_table,
    a.level::int as level_num,
    ol.name as level_name,
    COUNT(*) as count
FROM mun_hierarchy mh
JOIN addr_obj a ON mh.objectid = a.objectid AND a.isactual = 1
LEFT JOIN object_levels ol ON a.level::int = ol.level
WHERE mh.path LIKE '1405113.95251365%'  -- ПАРАМЕТР: path округа + %
  AND mh.isactive = 1
GROUP BY a.level::int, ol.name

UNION ALL

SELECT
    'houses' as source_table,
    10 as level_num,
    'Здание (дом)' as level_name,
    COUNT(*) as count
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
WHERE mh.path LIKE '1405113.95251365%'  -- ПАРАМЕТР: path округа + %
  AND h.isactual = 1
  AND mh.isactive = 1

ORDER BY level_num;

-- Результат для округа Троицк:
--  source_table | level_num |           level_name            | count
-- --------------+-----------+---------------------------------+-------
--  addr_obj     |         3 | Муниципальный район             |     1
--  addr_obj     |         5 | Город                           |     1
--  addr_obj     |         6 | Населенный пункт                |    20
--  addr_obj     |         7 | Элемент планировочной структуры |   172
--  addr_obj     |         8 | Элемент улично-дорожной сети    |   303
--  houses       |        10 | Здание (дом)                    | 14217
--
-- ВСЕГО в округе Троицк: 14,714 объектов


-- 11E. Найти конкретный дом по адресу
SELECT
    h.objectid,
    h.housenum,
    h.addnum1,
    h.addnum2,
    h.housetype,
    a.name as street_name,
    a.typename as street_type
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
JOIN addr_obj a ON mh.parentobjid = a.objectid
WHERE a.name = 'Арбат'  -- ПАРАМЕТР: название улицы
  AND a.typename = 'ул'  -- ПАРАМЕТР: тип объекта
  AND h.housenum = '10'  -- ПАРАМЕТР: номер дома
  AND h.isactual = 1
  AND a.isactual = 1
  AND mh.isactive = 1;


-- 11F. Полный адрес дома (используя функцию из ADDRESS_QUERIES.sql)
SELECT
    h.objectid,
    get_house_address(h.objectid) as full_address
FROM houses h
WHERE h.objectid = 67054556  -- ПАРАМЕТР: objectid дома
  AND h.isactual = 1;

-- Результат:
--  objectid | full_address
-- ----------+-------------------------------------------------------
--  67054556 | г Москва, вн.тер.г. муниципальный округ Арбат, ул Арбат, д. 10


-- 11G. Полный адрес дома БЕЗ функции (уровни 1-10 в одной строке)
-- Если функция get_house_address недоступна, используйте этот запрос

WITH house_path AS (
    -- Получаем дом и его путь через улицу
    SELECT
        h.objectid as house_objectid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        mh.path as house_path,
        mh.parentobjid as street_objectid
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    WHERE h.objectid = 67054556  -- ПАРАМЕТР: objectid дома
      AND h.isactual = 1
      AND mh.isactive = 1
),
street_path AS (
    -- Получаем путь улицы (который ведет до Москвы - уровень 1)
    SELECT
        hp.house_objectid,
        hp.housenum,
        hp.addnum1,
        hp.addnum2,
        mh.path as full_path
    FROM house_path hp
    JOIN mun_hierarchy mh ON hp.street_objectid = mh.objectid
    WHERE mh.isactive = 1
),
path_elements AS (
    -- Разбираем путь на элементы (уровни 1-8)
    SELECT
        sp.house_objectid,
        sp.housenum,
        sp.addnum1,
        sp.addnum2,
        unnest(string_to_array(sp.full_path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(sp.full_path, '.'), 1) as element_position
    FROM street_path sp
)
-- Собираем полный адрес (уровни 1-8 + уровень 10 дом)
SELECT
    pe.house_objectid,
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY pe.element_position
    ) || ', д. ' || pe.housenum ||
    CASE
        WHEN pe.addnum1 IS NOT NULL THEN ' корп. ' || pe.addnum1
        ELSE ''
    END ||
    CASE
        WHEN pe.addnum2 IS NOT NULL THEN ' стр. ' || pe.addnum2
        ELSE ''
    END as full_address_level_1_to_10
FROM path_elements pe
JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1
GROUP BY pe.house_objectid, pe.housenum, pe.addnum1, pe.addnum2;

-- Результат:
--  house_objectid |       full_address_level_1_to_10
-- ----------------+----------------------------------------------------------------
--        67054556 | г Москва, вн.тер.г. муниципальный округ Арбат, ул Арбат, д. 10
--
-- Этот адрес включает:
-- - Уровень 1: г Москва (Субъект РФ)
-- - Уровень 3: вн.тер.г. муниципальный округ Арбат (Муниципальный округ)
-- - Уровень 8: ул Арбат (Улица)
-- - Уровень 10: д. 10 (Дом)


-- ============================================================================
-- 12. ПОЛНАЯ ТАБЛИЦА ВСЕХ ДОМОВ С РАЗБИВКОЙ ПО УРОВНЯМ (1-10)
-- ============================================================================
-- Этот запрос возвращает ВСЕ дома Москвы (293,602 записи) с каждым уровнем
-- адреса в отдельной колонке + полный адрес в одной строке

WITH house_data AS (
    SELECT
        h.objectid as house_objectid,
        h.objectguid as house_objectguid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        h.housetype,
        mh.parentobjid as street_objectid,
        mh_street.path as street_path
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    JOIN mun_hierarchy mh_street ON mh.parentobjid = mh_street.objectid
    WHERE mh.path LIKE '1405113%'
      AND h.isactual = 1
      AND mh.isactive = 1
      AND mh_street.isactive = 1
),
path_expanded AS (
    SELECT
        hd.house_objectid,
        hd.house_objectguid,
        hd.housenum,
        hd.addnum1,
        hd.addnum2,
        hd.housetype,
        hd.street_objectid,
        unnest(string_to_array(hd.street_path, '.'))::bigint as path_objectid,
        generate_subscripts(string_to_array(hd.street_path, '.'), 1) as path_position
    FROM house_data hd
),
levels_pivoted AS (
    SELECT
        pe.house_objectid,
        pe.house_objectguid,
        pe.housenum,
        pe.addnum1,
        pe.addnum2,
        pe.housetype,
        -- Уровень 1 (Москва - Субъект РФ)
        MAX(CASE WHEN a.level::int = 1 THEN pe.path_objectid END) as level_1_objectid,
        MAX(CASE WHEN a.level::int = 1 THEN a.objectguid END) as level_1_objectguid,
        MAX(CASE WHEN a.level::int = 1 THEN a.name END) as level_1_name,
        MAX(CASE WHEN a.level::int = 1 THEN a.typename END) as level_1_typename,
        -- Уровень 3 (Муниципальный округ)
        MAX(CASE WHEN a.level::int = 3 THEN pe.path_objectid END) as level_3_objectid,
        MAX(CASE WHEN a.level::int = 3 THEN a.objectguid END) as level_3_objectguid,
        MAX(CASE WHEN a.level::int = 3 THEN a.name END) as level_3_name,
        MAX(CASE WHEN a.level::int = 3 THEN a.typename END) as level_3_typename,
        -- Уровень 5 (Город) - может быть NULL
        MAX(CASE WHEN a.level::int = 5 THEN pe.path_objectid END) as level_5_objectid,
        MAX(CASE WHEN a.level::int = 5 THEN a.objectguid END) as level_5_objectguid,
        MAX(CASE WHEN a.level::int = 5 THEN a.name END) as level_5_name,
        MAX(CASE WHEN a.level::int = 5 THEN a.typename END) as level_5_typename,
        -- Уровень 6 (Населенный пункт) - может быть NULL
        MAX(CASE WHEN a.level::int = 6 THEN pe.path_objectid END) as level_6_objectid,
        MAX(CASE WHEN a.level::int = 6 THEN a.objectguid END) as level_6_objectguid,
        MAX(CASE WHEN a.level::int = 6 THEN a.name END) as level_6_name,
        MAX(CASE WHEN a.level::int = 6 THEN a.typename END) as level_6_typename,
        -- Уровень 7 (Элемент планировки) - может быть NULL
        MAX(CASE WHEN a.level::int = 7 THEN pe.path_objectid END) as level_7_objectid,
        MAX(CASE WHEN a.level::int = 7 THEN a.objectguid END) as level_7_objectguid,
        MAX(CASE WHEN a.level::int = 7 THEN a.name END) as level_7_name,
        MAX(CASE WHEN a.level::int = 7 THEN a.typename END) as level_7_typename,
        -- Уровень 8 (Улица)
        MAX(CASE WHEN a.level::int = 8 THEN pe.path_objectid END) as level_8_objectid,
        MAX(CASE WHEN a.level::int = 8 THEN a.objectguid END) as level_8_objectguid,
        MAX(CASE WHEN a.level::int = 8 THEN a.name END) as level_8_name,
        MAX(CASE WHEN a.level::int = 8 THEN a.typename END) as level_8_typename,
        -- Для полного адреса
        string_agg(
            CASE
                WHEN a.typename IS NOT NULL AND a.typename != ''
                THEN a.typename || ' ' || a.name
                ELSE a.name
            END,
            ', '
            ORDER BY pe.path_position
        ) as address_parts
    FROM path_expanded pe
    JOIN addr_obj a ON pe.path_objectid = a.objectid AND a.isactual = 1
    GROUP BY pe.house_objectid, pe.house_objectguid, pe.housenum, pe.addnum1, pe.addnum2, pe.housetype
)
SELECT
    -- Уровень 10 (Дом)
    house_objectid as level_10_objectid,
    house_objectguid as level_10_objectguid,
    housenum as level_10_housenum,
    addnum1 as level_10_addnum1,
    addnum2 as level_10_addnum2,
    housetype as level_10_housetype,
    -- Уровень 1 (Москва)
    level_1_objectid,
    level_1_objectguid,
    level_1_typename,
    level_1_name,
    -- Уровень 3 (Округ)
    level_3_objectid,
    level_3_objectguid,
    level_3_typename,
    level_3_name,
    -- Уровень 5 (Город)
    level_5_objectid,
    level_5_objectguid,
    level_5_typename,
    level_5_name,
    -- Уровень 6 (Населенный пункт)
    level_6_objectid,
    level_6_objectguid,
    level_6_typename,
    level_6_name,
    -- Уровень 7 (Планировка)
    level_7_objectid,
    level_7_objectguid,
    level_7_typename,
    level_7_name,
    -- Уровень 8 (Улица)
    level_8_objectid,
    level_8_objectguid,
    level_8_typename,
    level_8_name,
    -- Полный адрес в одной строке
    address_parts || ', д. ' || housenum ||
    CASE WHEN addnum1 IS NOT NULL THEN ' корп. ' || addnum1 ELSE '' END ||
    CASE WHEN addnum2 IS NOT NULL THEN ' стр. ' || addnum2 ELSE '' END
    as full_address
FROM levels_pivoted
ORDER BY level_3_name, level_8_name, housenum;

-- Результат: 293,602 записи (все дома Москвы)
--
-- Структура результата:
-- - level_10_* : данные дома (objectid, objectguid, housenum, addnum1, addnum2, housetype)
-- - level_1_*  : Москва (objectid, objectguid, typename, name)
-- - level_3_*  : Округ (objectid, objectguid, typename, name)
-- - level_5_*  : Город (может быть NULL)
-- - level_6_*  : Населенный пункт (может быть NULL)
-- - level_7_*  : Планировка (может быть NULL)
-- - level_8_*  : Улица (objectid, objectguid, typename, name)
-- - full_address : полный адрес строкой
--
-- Пример записи:
-- level_10_objectid: 81707887
-- level_1_name: Москва
-- level_3_name: городской округ Троицк
-- level_6_name: Десна
-- level_7_name: 1 (микрорайон)
-- full_address: г Москва, вн.тер.г. городской округ Троицк, д Десна, мкр 1, д. 1


-- ============================================================================
-- ПРИМЕЧАНИЯ
-- ============================================================================
--
-- Уровни объектов в Москве:
-- 1  - Субъект РФ (Москва) - 1 объект [addr_obj]
-- 3  - Муниципальный район/округ - 136 объектов [addr_obj]
-- 5  - Город - 8 объектов [addr_obj]
-- 6  - Населенный пункт - 341 объект [addr_obj]
-- 7  - Элемент планировочной структуры - 2,105 объектов [addr_obj]
-- 8  - Элемент улично-дорожной сети - 6,306 объектов [addr_obj]
-- 10 - Здание (дом) - 293,602 объекта [houses] ⚠️ отдельная таблица!
--
-- ВСЕГО: 302,499 объектов (8,897 в addr_obj + 293,602 в houses)
--
-- ⚠️ ВАЖНО: Дома хранятся в отдельной таблице houses (не в addr_obj)!
--
-- Варианты фильтрации:
--
-- 1. ТОЛЬКО МУНИЦИПАЛЬНОЕ ДЕЛЕНИЕ (БЕЗ улиц, планировки и домов):
--    WHERE a.level::int <= 6
--    Результат: 486 объектов (1 + 136 + 8 + 341)
--
-- 2. ПОЛНАЯ СТРУКТУРА addr_obj (включая улицы и планировку, БЕЗ домов):
--    БЕЗ условия на level (или WHERE a.level::int <= 8)
--    Результат: 8,897 объектов
--
-- 3. СО ВСЕМИ ОБЪЕКТАМИ ВКЛЮЧАЯ ДОМА:
--    Нужно делать UNION ALL между addr_obj и houses (см. запрос 11D)
--    Результат: 302,499 объектов
--
-- 4. ТОЛЬКО ОКРУГА:
--    WHERE a.level::int = 3
--    Результат: 136 объектов
--
-- 5. ТОЛЬКО ДОМА:
--    SELECT FROM houses (не addr_obj)
--    Результат: 293,602 дома
--
-- 6. ДО ОПРЕДЕЛЕННОГО УРОВНЯ:
--    WHERE a.level::int <= N  (где N - нужный уровень)
--
-- Пример для округа Троицк:
-- - БЕЗ ДОМОВ: 497 объектов (1 округ + 1 город + 20 деревень + 172 планировки + 303 улицы)
-- - ТОЛЬКО МУН. ДЕЛЕНИЕ (level <= 6): 22 объекта (1 округ + 1 город + 20 деревень)
-- - С ДОМАМИ: 14,714 объектов (497 + 14,217 домов)
--

-- ============================================================================
-- 13. ПОЛНАЯ ТАБЛИЦА ВСЕХ КВАРТИР С РАЗБИВКОЙ ПО УРОВНЯМ (1-10 + 9 квартира)
-- ============================================================================
-- Этот запрос возвращает ВСЕ квартиры Москвы с каждым уровнем адреса
-- в отдельной колонке + полный адрес в одной строке
--
-- Использует раздел 12 как базу и добавляет квартиры через LEFT JOIN

WITH house_data AS (
    SELECT
        h.objectid as house_objectid,
        h.objectguid as house_objectguid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        h.housetype,
        mh.parentobjid as street_objectid,
        mh_street.path as street_path
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    JOIN mun_hierarchy mh_street ON mh.parentobjid = mh_street.objectid
    WHERE mh.path LIKE '1405113%'
      AND h.isactual = 1
      AND mh.isactive = 1
      AND mh_street.isactive = 1
),
apartment_data AS (
    SELECT
        hd.*,
        apt.objectid as apartment_objectid,
        apt.objectguid as apartment_objectguid,
        apt.number as apartment_number,
        apt.aparttype as apartment_type
    FROM house_data hd
    LEFT JOIN mun_hierarchy mh_apt ON mh_apt.parentobjid = hd.house_objectid AND mh_apt.isactive = 1
    LEFT JOIN apartments apt ON mh_apt.objectid = apt.objectid AND apt.isactual = 1
),
path_expanded AS (
    SELECT
        ad.house_objectid,
        ad.house_objectguid,
        ad.housenum,
        ad.addnum1,
        ad.addnum2,
        ad.housetype,
        ad.apartment_objectid,
        ad.apartment_objectguid,
        ad.apartment_number,
        ad.apartment_type,
        ad.street_objectid,
        unnest(string_to_array(ad.street_path, '.'))::bigint as path_objectid,
        generate_subscripts(string_to_array(ad.street_path, '.'), 1) as path_position
    FROM apartment_data ad
),
levels_pivoted AS (
    SELECT
        pe.house_objectid,
        pe.house_objectguid,
        pe.housenum,
        pe.addnum1,
        pe.addnum2,
        pe.housetype,
        pe.apartment_objectid,
        pe.apartment_objectguid,
        pe.apartment_number,
        pe.apartment_type,
        -- Pivot each level into separate columns
        MAX(CASE WHEN a.level::int = 1 THEN pe.path_objectid END) as level_1_objectid,
        MAX(CASE WHEN a.level::int = 1 THEN a.objectguid END) as level_1_objectguid,
        MAX(CASE WHEN a.level::int = 1 THEN a.typename END) as level_1_typename,
        MAX(CASE WHEN a.level::int = 1 THEN a.name END) as level_1_name,
        MAX(CASE WHEN a.level::int = 3 THEN pe.path_objectid END) as level_3_objectid,
        MAX(CASE WHEN a.level::int = 3 THEN a.objectguid END) as level_3_objectguid,
        MAX(CASE WHEN a.level::int = 3 THEN a.typename END) as level_3_typename,
        MAX(CASE WHEN a.level::int = 3 THEN a.name END) as level_3_name,
        MAX(CASE WHEN a.level::int = 5 THEN pe.path_objectid END) as level_5_objectid,
        MAX(CASE WHEN a.level::int = 5 THEN a.objectguid END) as level_5_objectguid,
        MAX(CASE WHEN a.level::int = 5 THEN a.typename END) as level_5_typename,
        MAX(CASE WHEN a.level::int = 5 THEN a.name END) as level_5_name,
        MAX(CASE WHEN a.level::int = 6 THEN pe.path_objectid END) as level_6_objectid,
        MAX(CASE WHEN a.level::int = 6 THEN a.objectguid END) as level_6_objectguid,
        MAX(CASE WHEN a.level::int = 6 THEN a.typename END) as level_6_typename,
        MAX(CASE WHEN a.level::int = 6 THEN a.name END) as level_6_name,
        MAX(CASE WHEN a.level::int = 7 THEN pe.path_objectid END) as level_7_objectid,
        MAX(CASE WHEN a.level::int = 7 THEN a.objectguid END) as level_7_objectguid,
        MAX(CASE WHEN a.level::int = 7 THEN a.typename END) as level_7_typename,
        MAX(CASE WHEN a.level::int = 7 THEN a.name END) as level_7_name,
        MAX(CASE WHEN a.level::int = 8 THEN pe.path_objectid END) as level_8_objectid,
        MAX(CASE WHEN a.level::int = 8 THEN a.objectguid END) as level_8_objectguid,
        MAX(CASE WHEN a.level::int = 8 THEN a.typename END) as level_8_typename,
        MAX(CASE WHEN a.level::int = 8 THEN a.name END) as level_8_name,
        -- Build address parts string (without house and apartment)
        string_agg(
            CASE
                WHEN a.typename IS NOT NULL AND a.typename != ''
                THEN a.typename || ' ' || a.name
                ELSE a.name
            END,
            ', '
            ORDER BY pe.path_position
        ) as address_parts
    FROM path_expanded pe
    JOIN addr_obj a ON pe.path_objectid = a.objectid AND a.isactual = 1
    GROUP BY
        pe.house_objectid, pe.house_objectguid, pe.housenum, pe.addnum1, pe.addnum2, pe.housetype,
        pe.apartment_objectid, pe.apartment_objectguid, pe.apartment_number, pe.apartment_type
)
SELECT
    -- Apartment (level 9) columns
    apartment_objectid as level_9_objectid,
    apartment_objectguid as level_9_objectguid,
    apartment_number as level_9_number,
    apartment_type as level_9_aparttype,
    -- House (level 10) columns
    house_objectid as level_10_objectid,
    house_objectguid as level_10_objectguid,
    housenum as level_10_housenum,
    addnum1 as level_10_addnum1,
    addnum2 as level_10_addnum2,
    housetype as level_10_housetype,
    -- Address hierarchy columns
    level_1_objectid,
    level_1_objectguid,
    level_1_typename,
    level_1_name,
    level_3_objectid,
    level_3_objectguid,
    level_3_typename,
    level_3_name,
    level_5_objectid,
    level_5_objectguid,
    level_5_typename,
    level_5_name,
    level_6_objectid,
    level_6_objectguid,
    level_6_typename,
    level_6_name,
    level_7_objectid,
    level_7_objectguid,
    level_7_typename,
    level_7_name,
    level_8_objectid,
    level_8_objectguid,
    level_8_typename,
    level_8_name,
    -- Full address with house and apartment
    address_parts || ', д. ' || housenum ||
    CASE WHEN addnum1 IS NOT NULL THEN ' корп. ' || addnum1 ELSE '' END ||
    CASE WHEN addnum2 IS NOT NULL THEN ' стр. ' || addnum2 ELSE '' END ||
    CASE WHEN apartment_number IS NOT NULL THEN ', кв. ' || apartment_number ELSE '' END
    as full_address
FROM levels_pivoted
WHERE apartment_objectid IS NOT NULL  -- Только записи с квартирами
ORDER BY level_3_name, level_8_name, housenum, apartment_number;

-- Примечание:
-- Этот запрос возвращает только квартиры (не дома без квартир)
-- Если нужны ВСЕ дома (в том числе без квартир), уберите WHERE apartment_objectid IS NOT NULL


-- ============================================================================
-- 14. ПОЛНАЯ ТАБЛИЦА ВСЕХ ПОМЕЩЕНИЙ С РАЗБИВКОЙ ПО УРОВНЯМ (1-10 + 9 + 65)
-- ============================================================================
-- Этот запрос возвращает ВСЕ помещения (rooms) Москвы с каждым уровнем адреса
-- в отдельной колонке + полный адрес в одной строке
--
-- Использует раздел 13 как базу и добавляет помещения через LEFT JOIN

WITH house_data AS (
    SELECT
        h.objectid as house_objectid,
        h.objectguid as house_objectguid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        h.housetype,
        mh.parentobjid as street_objectid,
        mh_street.path as street_path
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    JOIN mun_hierarchy mh_street ON mh.parentobjid = mh_street.objectid
    WHERE mh.path LIKE '1405113%'
      AND h.isactual = 1
      AND mh.isactive = 1
      AND mh_street.isactive = 1
),
apartment_data AS (
    SELECT
        hd.*,
        apt.objectid as apartment_objectid,
        apt.objectguid as apartment_objectguid,
        apt.number as apartment_number,
        apt.aparttype as apartment_type
    FROM house_data hd
    LEFT JOIN mun_hierarchy mh_apt ON mh_apt.parentobjid = hd.house_objectid AND mh_apt.isactive = 1
    LEFT JOIN apartments apt ON mh_apt.objectid = apt.objectid AND apt.isactual = 1
),
room_data AS (
    SELECT
        ad.*,
        r.objectid as room_objectid,
        r.objectguid as room_objectguid,
        r.number as room_number,
        r.roomtype as room_type
    FROM apartment_data ad
    LEFT JOIN mun_hierarchy mh_room ON mh_room.parentobjid = ad.apartment_objectid AND mh_room.isactive = 1
    LEFT JOIN rooms r ON mh_room.objectid = r.objectid AND r.isactual = 1
),
path_expanded AS (
    SELECT
        rd.house_objectid,
        rd.house_objectguid,
        rd.housenum,
        rd.addnum1,
        rd.addnum2,
        rd.housetype,
        rd.apartment_objectid,
        rd.apartment_objectguid,
        rd.apartment_number,
        rd.apartment_type,
        rd.room_objectid,
        rd.room_objectguid,
        rd.room_number,
        rd.room_type,
        rd.street_objectid,
        unnest(string_to_array(rd.street_path, '.'))::bigint as path_objectid,
        generate_subscripts(string_to_array(rd.street_path, '.'), 1) as path_position
    FROM room_data rd
),
levels_pivoted AS (
    SELECT
        pe.house_objectid,
        pe.house_objectguid,
        pe.housenum,
        pe.addnum1,
        pe.addnum2,
        pe.housetype,
        pe.apartment_objectid,
        pe.apartment_objectguid,
        pe.apartment_number,
        pe.apartment_type,
        pe.room_objectid,
        pe.room_objectguid,
        pe.room_number,
        pe.room_type,
        -- Pivot each level into separate columns
        MAX(CASE WHEN a.level::int = 1 THEN pe.path_objectid END) as level_1_objectid,
        MAX(CASE WHEN a.level::int = 1 THEN a.objectguid END) as level_1_objectguid,
        MAX(CASE WHEN a.level::int = 1 THEN a.typename END) as level_1_typename,
        MAX(CASE WHEN a.level::int = 1 THEN a.name END) as level_1_name,
        MAX(CASE WHEN a.level::int = 3 THEN pe.path_objectid END) as level_3_objectid,
        MAX(CASE WHEN a.level::int = 3 THEN a.objectguid END) as level_3_objectguid,
        MAX(CASE WHEN a.level::int = 3 THEN a.typename END) as level_3_typename,
        MAX(CASE WHEN a.level::int = 3 THEN a.name END) as level_3_name,
        MAX(CASE WHEN a.level::int = 5 THEN pe.path_objectid END) as level_5_objectid,
        MAX(CASE WHEN a.level::int = 5 THEN a.objectguid END) as level_5_objectguid,
        MAX(CASE WHEN a.level::int = 5 THEN a.typename END) as level_5_typename,
        MAX(CASE WHEN a.level::int = 5 THEN a.name END) as level_5_name,
        MAX(CASE WHEN a.level::int = 6 THEN pe.path_objectid END) as level_6_objectid,
        MAX(CASE WHEN a.level::int = 6 THEN a.objectguid END) as level_6_objectguid,
        MAX(CASE WHEN a.level::int = 6 THEN a.typename END) as level_6_typename,
        MAX(CASE WHEN a.level::int = 6 THEN a.name END) as level_6_name,
        MAX(CASE WHEN a.level::int = 7 THEN pe.path_objectid END) as level_7_objectid,
        MAX(CASE WHEN a.level::int = 7 THEN a.objectguid END) as level_7_objectguid,
        MAX(CASE WHEN a.level::int = 7 THEN a.typename END) as level_7_typename,
        MAX(CASE WHEN a.level::int = 7 THEN a.name END) as level_7_name,
        MAX(CASE WHEN a.level::int = 8 THEN pe.path_objectid END) as level_8_objectid,
        MAX(CASE WHEN a.level::int = 8 THEN a.objectguid END) as level_8_objectguid,
        MAX(CASE WHEN a.level::int = 8 THEN a.typename END) as level_8_typename,
        MAX(CASE WHEN a.level::int = 8 THEN a.name END) as level_8_name,
        -- Build address parts string (without house, apartment, room)
        string_agg(
            CASE
                WHEN a.typename IS NOT NULL AND a.typename != ''
                THEN a.typename || ' ' || a.name
                ELSE a.name
            END,
            ', '
            ORDER BY pe.path_position
        ) as address_parts
    FROM path_expanded pe
    JOIN addr_obj a ON pe.path_objectid = a.objectid AND a.isactual = 1
    GROUP BY
        pe.house_objectid, pe.house_objectguid, pe.housenum, pe.addnum1, pe.addnum2, pe.housetype,
        pe.apartment_objectid, pe.apartment_objectguid, pe.apartment_number, pe.apartment_type,
        pe.room_objectid, pe.room_objectguid, pe.room_number, pe.room_type
)
SELECT
    -- Room (level 65) columns
    room_objectid as level_65_objectid,
    room_objectguid as level_65_objectguid,
    room_number as level_65_number,
    room_type as level_65_roomtype,
    -- Apartment (level 9) columns
    apartment_objectid as level_9_objectid,
    apartment_objectguid as level_9_objectguid,
    apartment_number as level_9_number,
    apartment_type as level_9_aparttype,
    -- House (level 10) columns
    house_objectid as level_10_objectid,
    house_objectguid as level_10_objectguid,
    housenum as level_10_housenum,
    addnum1 as level_10_addnum1,
    addnum2 as level_10_addnum2,
    housetype as level_10_housetype,
    -- Address hierarchy columns
    level_1_objectid,
    level_1_objectguid,
    level_1_typename,
    level_1_name,
    level_3_objectid,
    level_3_objectguid,
    level_3_typename,
    level_3_name,
    level_5_objectid,
    level_5_objectguid,
    level_5_typename,
    level_5_name,
    level_6_objectid,
    level_6_objectguid,
    level_6_typename,
    level_6_name,
    level_7_objectid,
    level_7_objectguid,
    level_7_typename,
    level_7_name,
    level_8_objectid,
    level_8_objectguid,
    level_8_typename,
    level_8_name,
    -- Full address with house, apartment and room
    address_parts || ', д. ' || housenum ||
    CASE WHEN addnum1 IS NOT NULL THEN ' корп. ' || addnum1 ELSE '' END ||
    CASE WHEN addnum2 IS NOT NULL THEN ' стр. ' || addnum2 ELSE '' END ||
    CASE WHEN apartment_number IS NOT NULL THEN ', кв. ' || apartment_number ELSE '' END ||
    CASE WHEN room_number IS NOT NULL THEN ', пом. ' || room_number ELSE '' END
    as full_address
FROM levels_pivoted
WHERE room_objectid IS NOT NULL  -- Только записи с помещениями
ORDER BY level_3_name, level_8_name, housenum, apartment_number, room_number;

-- Примечание:
-- Этот запрос возвращает только помещения (не квартиры без помещений)
-- Если нужны ВСЕ квартиры (в том числе без помещений), уберите WHERE room_objectid IS NOT NULL
