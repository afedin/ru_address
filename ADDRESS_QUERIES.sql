-- ============================================================================
-- SQL запросы для построения полных адресов из базы данных ГАР
-- ============================================================================
--
-- Структура адреса в ГАР строится через муниципальную иерархию (mun_hierarchy),
-- которая содержит поле path с цепочкой objectid от региона до конечного объекта.
--
-- Основные таблицы:
-- - addr_obj: адресные объекты (регион, район, город, улица и т.д.)
-- - mun_hierarchy: муниципальная иерархия (содержит path для построения адреса)
-- - houses: дома
-- - apartments: квартиры
-- - object_levels: справочник уровней объектов
--
-- Уровни объектов:
-- 1  - Субъект РФ (регион)
-- 2  - Административный район
-- 3  - Муниципальный район
-- 4  - Сельское/городское поселение
-- 5  - Город
-- 6  - Населенный пункт
-- 7  - Элемент планировочной структуры (микрорайон)
-- 8  - Элемент улично-дорожной сети (улица, проспект, переулок)
-- 9  - Земельный участок
-- 10 - Здание (дом)
-- 11 - Помещение (квартира)
-- ============================================================================

-- ============================================================================
-- 1. РЕКУРСИВНЫЙ ЗАПРОС: Построение полного адреса по objectid адресного объекта
-- ============================================================================
-- Этот запрос строит полный адрес от региона до указанного объекта
-- используя поле path из mun_hierarchy

WITH RECURSIVE address_chain AS (
    -- Начальная точка: объект, для которого строим адрес
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int as level,
        1 as depth,
        ARRAY[h.objectid] as path_array
    FROM mun_hierarchy h
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.objectid = 1456293  -- ПАРАМЕТР: objectid объекта (например, улицы)
      AND h.isactive = 1

    UNION ALL

    -- Рекурсивная часть: поднимаемся вверх по иерархии
    SELECT
        h.objectid,
        h.parentobjid,
        h.path,
        a.name,
        a.typename,
        a.level::int,
        ac.depth + 1,
        ac.path_array || h.objectid
    FROM address_chain ac
    JOIN mun_hierarchy h ON h.objectid = ac.parentobjid
    JOIN addr_obj a ON h.objectid = a.objectid AND a.isactual = 1
    WHERE h.isactive = 1
)
SELECT
    objectid,
    level,
    typename,
    name,
    depth,
    -- Собираем адрес снизу вверх (от региона к объекту)
    string_agg(
        CASE
            WHEN typename IS NOT NULL AND typename != ''
            THEN typename || ' ' || name
            ELSE name
        END,
        ', '
        ORDER BY level
    ) OVER (PARTITION BY 1) as full_address
FROM address_chain
ORDER BY level;


-- ============================================================================
-- 2. УПРОЩЕННЫЙ ЗАПРОС: Построение адреса через поле path
-- ============================================================================
-- Более простой вариант: разбираем path и собираем адрес за один запрос

WITH path_elements AS (
    SELECT
        h.objectid,
        h.path,
        -- Разбиваем path на отдельные objectid
        unnest(string_to_array(h.path, '.'))::bigint as element_objectid,
        -- Сохраняем позицию элемента в пути
        generate_subscripts(string_to_array(h.path, '.'), 1) as element_position
    FROM mun_hierarchy h
    WHERE h.objectid = 1456293  -- ПАРАМЕТР: objectid объекта
      AND h.isactive = 1
)
SELECT
    pe.objectid as target_objectid,
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
JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1
GROUP BY pe.objectid;


-- ============================================================================
-- 3. АДРЕС ДОМА: Полный адрес с номером дома
-- ============================================================================
-- Строит адрес вида: "АО Ненецкий, г.о. город Нарьян-Мар, ул Юбилейная, д. 45"

WITH house_address AS (
    SELECT
        h.objectid as house_objectid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        mh.path,
        -- Разбираем путь на элементы
        unnest(string_to_array(mh.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(mh.path, '.'), 1) as element_position
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    WHERE h.objectid = 32269693  -- ПАРАМЕТР: objectid дома
      AND h.isactual = 1
      AND mh.isactive = 1
)
SELECT
    ha.house_objectid,
    ha.housenum,
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY ha.element_position
    ) || ', д. ' || ha.housenum ||
    CASE
        WHEN ha.addnum1 IS NOT NULL THEN ' корп. ' || ha.addnum1
        ELSE ''
    END ||
    CASE
        WHEN ha.addnum2 IS NOT NULL THEN ' стр. ' || ha.addnum2
        ELSE ''
    END as full_address
FROM house_address ha
JOIN addr_obj a ON ha.element_objectid = a.objectid AND a.isactual = 1
GROUP BY ha.house_objectid, ha.housenum, ha.addnum1, ha.addnum2;


-- ============================================================================
-- 4. АДРЕС КВАРТИРЫ: Полный адрес с домом и квартирой
-- ============================================================================
-- Строит адрес вида: "АО Ненецкий, ..., ул Юбилейная, д. 45, кв. 12"

WITH apartment_address AS (
    SELECT
        ap.objectid as apartment_objectid,
        ap.number as apartment_number,
        h.objectid as house_objectid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        mh.path,
        unnest(string_to_array(mh.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(mh.path, '.'), 1) as element_position
    FROM apartments ap
    JOIN mun_hierarchy apmh ON ap.objectid = apmh.objectid
    JOIN mun_hierarchy mh ON apmh.parentobjid = mh.objectid
    JOIN houses h ON mh.objectid = h.objectid AND h.isactual = 1
    WHERE ap.objectid = 12345678  -- ПАРАМЕТР: objectid квартиры
      AND ap.isactual = 1
      AND mh.isactive = 1
)
SELECT
    aa.apartment_objectid,
    aa.apartment_number,
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY aa.element_position
    ) || ', д. ' || aa.housenum ||
    CASE
        WHEN aa.addnum1 IS NOT NULL THEN ' корп. ' || aa.addnum1
        ELSE ''
    END ||
    CASE
        WHEN aa.addnum2 IS NOT NULL THEN ' стр. ' || aa.addnum2
        ELSE ''
    END ||
    ', кв. ' || aa.apartment_number as full_address
FROM apartment_address aa
JOIN addr_obj a ON aa.element_objectid = a.objectid AND a.isactual = 1
GROUP BY aa.apartment_objectid, aa.apartment_number, aa.housenum, aa.addnum1, aa.addnum2;


-- ============================================================================
-- 5. ПОИСК ПО ЧАСТИ АДРЕСА: Найти все дома на улице
-- ============================================================================
-- Находит все дома на указанной улице (или другом объекте)

SELECT
    h.objectid as house_objectid,
    h.housenum,
    h.addnum1,
    h.addnum2,
    a.name as street_name,
    a.typename as street_type,
    string_agg(
        DISTINCT CASE
            WHEN a2.typename IS NOT NULL AND a2.typename != '' AND a2.level::int <= 6
            THEN a2.typename || ' ' || a2.name
            ELSE a2.name
        END,
        ', '
        ORDER BY CASE
            WHEN a2.typename IS NOT NULL AND a2.typename != '' AND a2.level::int <= 6
            THEN a2.typename || ' ' || a2.name
            ELSE a2.name
        END
    ) as parent_address
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
JOIN addr_obj a ON mh.parentobjid = a.objectid AND a.isactual = 1
-- Разбираем path для получения родительских объектов
JOIN LATERAL (
    SELECT unnest(string_to_array(mh.path, '.'))::bigint as parent_objectid
) parents ON true
JOIN addr_obj a2 ON parents.parent_objectid = a2.objectid
    AND a2.isactual = 1
    AND a2.level::int <= 6  -- Только уровни до населенного пункта
WHERE a.name = 'Юбилейная'  -- ПАРАМЕТР: название улицы
  AND a.typename = 'ул'      -- ПАРАМЕТР: тип объекта
  AND a.level = '8'
  AND h.isactual = 1
  AND mh.isactive = 1
GROUP BY h.objectid, h.housenum, h.addnum1, h.addnum2, a.name, a.typename
ORDER BY h.housenum;


-- ============================================================================
-- 6. ФУНКЦИЯ: Получение полного адреса по objectid
-- ============================================================================
-- Создаёт функцию для удобного получения адреса

CREATE OR REPLACE FUNCTION get_full_address(p_objectid bigint)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_address text;
    v_path text;
BEGIN
    -- Получаем path
    SELECT h.path INTO v_path
    FROM mun_hierarchy h
    WHERE h.objectid = p_objectid
      AND h.isactive = 1
    LIMIT 1;

    IF v_path IS NULL THEN
        RETURN 'Адрес не найден';
    END IF;

    -- Разбираем path и собираем адрес
    WITH path_elements AS (
        SELECT
            unnest(string_to_array(v_path, '.'))::bigint as element_objectid,
            generate_subscripts(string_to_array(v_path, '.'), 1) as element_position
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
        ) INTO v_address
    FROM path_elements pe
    JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1;

    RETURN COALESCE(v_address, 'Адрес не найден');
END;
$$;

-- Пример использования функции:
-- SELECT get_full_address(1456293);
-- Результат: "АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная"


-- ============================================================================
-- 7. ФУНКЦИЯ: Получение адреса дома с номером
-- ============================================================================

CREATE OR REPLACE FUNCTION get_house_address(p_house_objectid bigint)
RETURNS text
LANGUAGE plpgsql
AS $$
DECLARE
    v_address text;
    v_path text;
    v_housenum varchar(50);
    v_addnum1 varchar(50);
    v_addnum2 varchar(50);
BEGIN
    -- Получаем данные о доме и path
    SELECT h.housenum, h.addnum1, h.addnum2, mh.path
    INTO v_housenum, v_addnum1, v_addnum2, v_path
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    WHERE h.objectid = p_house_objectid
      AND h.isactual = 1
      AND mh.isactive = 1
    LIMIT 1;

    IF v_path IS NULL THEN
        RETURN 'Адрес не найден';
    END IF;

    -- Собираем адрес
    WITH path_elements AS (
        SELECT
            unnest(string_to_array(v_path, '.'))::bigint as element_objectid,
            generate_subscripts(string_to_array(v_path, '.'), 1) as element_position
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
        ) INTO v_address
    FROM path_elements pe
    JOIN addr_obj a ON pe.element_objectid = a.objectid AND a.isactual = 1;

    -- Добавляем номер дома
    v_address := v_address || ', д. ' || v_housenum;

    IF v_addnum1 IS NOT NULL THEN
        v_address := v_address || ' корп. ' || v_addnum1;
    END IF;

    IF v_addnum2 IS NOT NULL THEN
        v_address := v_address || ' стр. ' || v_addnum2;
    END IF;

    RETURN COALESCE(v_address, 'Адрес не найден');
END;
$$;

-- Пример использования:
-- SELECT get_house_address(32269693);
-- Результат: "АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная, д. 45"


-- ============================================================================
-- 8. ПРЕДСТАВЛЕНИЕ: Все актуальные дома с полными адресами
-- ============================================================================
-- Создаёт представление со всеми домами и их адресами
-- ВНИМАНИЕ: Может быть медленным на больших объёмах данных!

CREATE OR REPLACE VIEW v_houses_with_addresses AS
WITH house_paths AS (
    SELECT
        h.objectid as house_objectid,
        h.housenum,
        h.addnum1,
        h.addnum2,
        mh.path,
        unnest(string_to_array(mh.path, '.'))::bigint as element_objectid,
        generate_subscripts(string_to_array(mh.path, '.'), 1) as element_position
    FROM houses h
    JOIN mun_hierarchy mh ON h.objectid = mh.objectid
    WHERE h.isactual = 1
      AND mh.isactive = 1
)
SELECT
    hp.house_objectid,
    hp.housenum,
    hp.addnum1,
    hp.addnum2,
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY hp.element_position
    ) as address_without_house,
    string_agg(
        CASE
            WHEN a.typename IS NOT NULL AND a.typename != ''
            THEN a.typename || ' ' || a.name
            ELSE a.name
        END,
        ', '
        ORDER BY hp.element_position
    ) || ', д. ' || hp.housenum ||
    CASE
        WHEN hp.addnum1 IS NOT NULL THEN ' корп. ' || hp.addnum1
        ELSE ''
    END ||
    CASE
        WHEN hp.addnum2 IS NOT NULL THEN ' стр. ' || hp.addnum2
        ELSE ''
    END as full_address
FROM house_paths hp
JOIN addr_obj a ON hp.element_objectid = a.objectid AND a.isactual = 1
GROUP BY hp.house_objectid, hp.housenum, hp.addnum1, hp.addnum2;

-- Пример использования:
-- SELECT * FROM v_houses_with_addresses WHERE housenum = '45' LIMIT 10;


-- ============================================================================
-- 9. ПОИСК АДРЕСА: По названию улицы и номеру дома
-- ============================================================================

SELECT
    h.objectid,
    get_house_address(h.objectid) as full_address
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
JOIN addr_obj a ON mh.parentobjid = a.objectid
WHERE a.name ILIKE '%Юбилейная%'  -- ПАРАМЕТР: название улицы (поиск без учета регистра)
  AND a.level = '8'
  AND h.housenum = '45'            -- ПАРАМЕТР: номер дома
  AND h.isactual = 1
  AND a.isactual = 1
  AND mh.isactive = 1
LIMIT 10;


-- ============================================================================
-- 10. СТАТИСТИКА: Количество адресных объектов по уровням
-- ============================================================================

SELECT
    a.level::int,
    ol.name as level_name,
    COUNT(*) as total_objects,
    COUNT(*) FILTER (WHERE a.isactual = 1) as actual_objects,
    COUNT(*) FILTER (WHERE a.isactive = 1) as active_objects
FROM addr_obj a
LEFT JOIN object_levels ol ON a.level::int = ol.level
GROUP BY a.level::int, ol.name
ORDER BY a.level::int;


-- ============================================================================
-- ТЕСТОВЫЕ ЗАПРОСЫ И РЕЗУЛЬТАТЫ
-- ============================================================================

-- Тест 1: Получить адрес улицы
SELECT get_full_address(1456293);
-- Результат:
-- АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная

-- Тест 2: Получить адрес города
SELECT get_full_address(1447228);
-- Результат:
-- АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар

-- Тест 3: Получить адрес дома
SELECT get_house_address(32269693);
-- Результат:
-- АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная, д. 45

SELECT get_house_address(32264421);
-- Результат:
-- АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная, д. 11

-- Тест 4: Найти все дома на улице (через v_houses_with_addresses)
SELECT * FROM v_houses_with_addresses
WHERE address_without_house ILIKE '%Юбилейная%'
ORDER BY housenum
LIMIT 10;
-- Результаты:
-- house_objectid | housenum | address_without_house | full_address
-- 32322779       | 10       | АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная | ..., д. 10
-- 97048584       | 10       | АО Ненецкий, м.р-н..., с Несь, ул Юбилейная | ..., д. 10
-- и т.д.

-- Тест 5: Поиск по части адреса
SELECT
    house_objectid,
    full_address
FROM v_houses_with_addresses
WHERE full_address ILIKE '%Нарьян-Мар%'
  AND full_address ILIKE '%Юбилейная%'
ORDER BY housenum
LIMIT 5;

-- Тест 6: Прямой запрос - все дома на улице Юбилейная
SELECT
    h.objectid as house_objectid,
    h.housenum,
    get_house_address(h.objectid) as full_address
FROM houses h
JOIN mun_hierarchy mh ON h.objectid = mh.objectid
JOIN addr_obj a ON mh.parentobjid = a.objectid
WHERE a.name = 'Юбилейная'
  AND a.typename = 'ул'
  AND a.level = '8'
  AND h.isactual = 1
  AND a.isactual = 1
  AND mh.isactive = 1
ORDER BY h.housenum
LIMIT 10;
-- Результат: список домов с полными адресами


-- ============================================================================
-- ПРИМЕРЫ РЕАЛЬНЫХ РЕЗУЛЬТАТОВ
-- ============================================================================

/*
ПРИМЕР 1: Упрощенный запрос для построения адреса улицы
---------------------------------------------------------
target_objectid |                          full_address
-----------------+----------------------------------------------------------------
         1456293 | АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная


ПРИМЕР 2: Адрес дома
---------------------
 house_objectid | housenum |                             full_address
----------------+----------+-----------------------------------------------------------------------
       32269693 | 45       | АО Ненецкий, г.о. город Нарьян-Мар, г Нарьян-Мар, ул Юбилейная, д. 45


ПРИМЕР 3: Все дома на улице Юбилейная (первые 10)
---------------------------------------------------
 house_objectid | housenum | addnum1 | addnum2 | street_name | street_type |                         parent_address
----------------+----------+---------+---------+-------------+-------------+--------------------------------------------------------
       32322779 | 10       |         |         | Юбилейная   | ул          | АО Ненецкий, г Нарьян-Мар, г.о. город Нарьян-Мар
       97048584 | 10       |         |         | Юбилейная   | ул          | АО Ненецкий, м.р-н Муниципальный район Заполярный район, с Несь, с.п. Канинский сельсовет
       96750499 | 100      |         |         | Юбилейная   | ул          | АО Ненецкий, г.п. рабочий поселок Искателей, м.р-н Муниципальный район Заполярный район, рп Искателей
      102985973 | 104      |         |         | Юбилейная   | ул          | АО Ненецкий, г.п. рабочий поселок Искателей, м.р-н Муниципальный район Заполярный район, рп Искателей
      169254111 | 10А      |         |         | Юбилейная   | ул          | АО Ненецкий, г Нарьян-Мар, г.о. город Нарьян-Мар
       32264421 | 11       |         |         | Юбилейная   | ул          | АО Ненецкий, г Нарьян-Мар, г.о. город Нарьян-Мар


ПРИМЕР 4: Статистика по уровням объектов
------------------------------------------
 level |                            level_name                             | total_objects | actual_objects | active_objects
-------+-------------------------------------------------------------------+---------------+----------------+----------------
     1 | Субъект РФ                                                        |             1 |              1 |              1
     2 | Административный район                                            |             1 |              1 |              1
     3 | Муниципальный район                                               |             3 |              3 |              3
     4 | Сельское/городское поселение                                      |            18 |             18 |             18
     5 | Город                                                             |            17 |             17 |             17
     6 | Населенный пункт                                                  |           132 |            132 |            132
     7 | Элемент планировочной структуры                                   |             7 |              7 |              7
     8 | Элемент улично-дорожной сети                                      |           329 |            329 |            329
*/
