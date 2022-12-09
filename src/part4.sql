-- 1) Создать хранимую процедуру, которая, не уничтожая базу данных, уничтожает все те таблицы текущей базы данных, имена которых начинаются с фразы 'TableName'.
CREATE OR REPLACE PROCEDURE pr_remove_table_like(IN tablename_pattern text)
LANGUAGE plpgsql AS $$
DECLARE
    tablenames text;
BEGIN
FOR tablenames  IN
    SELECT quote_ident(table_schema) || '.' || quote_ident(table_name)
    FROM   information_schema.tables
    WHERE  table_name LIKE tablename_pattern || '%'
    AND    table_schema NOT LIKE 'pg_%'
LOOP
EXECUTE 'DROP TABLE ' || tablenames;
END LOOP;
END
$$;

do $$
DECLARE
BEGIN
    CALL pr_remove_table_like('time');
END $$;

-- 2) Создать хранимую процедуру с выходным параметром, которая выводит список имен и параметров всех скалярных SQL функций пользователя в текущей базе данных.
CREATE OR REPLACE PROCEDURE pr_getCountNamesAndParameters(IN ref refcursor, INOUT func_count integer = 0)
LANGUAGE plpgsql as $$
BEGIN
OPEN ref FOR
-- найти имя функций и список ее параметров
with cte as (
select r.routine_name, string_agg(p.parameter_name, ',')
from information_schema.routines r
join information_schema.parameters p on r.specific_name = p.specific_name and p.specific_schema='public' and p.parameter_mode='IN'
where routine_type='FUNCTION'
group by routine_name
) select * from cte;

MOVE FORWARD ALL FROM ref; -- перемещает курсор, не получая данные
GET DIAGNOSTICS func_count := ROW_COUNT; -- число строк, обработанных последней командой SQL
MOVE BACKWARD ALL FROM ref; -- возвращаем курсор назад
END; $$;

BEGIN;
call pr_getCountNamesAndParameters('cursor');
FETCH ALL IN "cursor";
COMMIT;

do $$
DECLARE
    procedure_out int := 0;
BEGIN
    CALL pr_getCountNamesAndParameters('cursor', procedure_out);
    raise notice 'procedure result: %', procedure_out;
END $$;

-- 3) Создать хранимую процедуру с выходным параметром, которая уничтожает все SQL DDL триггеры в текущей базе данных.
CREATE OR REPLACE PROCEDURE pr_kill_DML_triggers(INOUT result integer)
LANGUAGE plpgsql as $$
DECLARE
    triggers_and_tables_names text;
BEGIN
FOR triggers_and_tables_names IN
    SELECT trigger_name || ' ON ' || event_object_table
    FROM information_schema.triggers
    WHERE trigger_schema = 'public'
        and event_manipulation in('INSERT', 'DELETE', 'UPDATE')
LOOP
    EXECUTE 'DROP TRIGGER ' || triggers_and_tables_names;
    result := result+1;
END LOOP;

END $$;

do $$
DECLARE
    procedure_result int := 0;
BEGIN
    CALL pr_kill_DML_triggers(procedure_result);
    raise notice 'result: %', procedure_result;
END $$;

select trigger_name
from information_schema.triggers;  -- проверяем что удалилось

-- 4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов
CREATE or replace PROCEDURE pr_show_names_and_type_obj(IN ref refcursor, IN search_string text)
LANGUAGE plpgsql as $$
BEGIN
OPEN ref FOR
    SELECT routine_name, routine_type
    FROM information_schema.routines
    WHERE specific_schema='public'
    and routine_definition like '%' || search_string || '%';
END $$;

BEGIN;
call pr_show_names_and_type_obj('cursor_name', 'Success');
FETCH ALL IN "cursor_name";
COMMIT;
