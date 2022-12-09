-- 1) Написать процедуру добавления P2P проверки
CREATE OR REPLACE PROCEDURE pr_p2p_insert(IN peer_checked_ text, IN peer_checking_ text, IN task_ varchar, IN status_ check_status, IN time_ time)
LANGUAGE plpgsql as
$$
DECLARE
    check_id integer := 0;
BEGIN
    IF status_ = 'Start' THEN -- Если задан статус "начало", добавить запись в таблицу Checks (в качестве даты использовать сегодняшнюю)
        IF NOT EXISTS(
                        SELECT checks.id, p1.state
                        FROM checks
                        JOIN p2p p1 ON checks.id = p1."check" AND p1.state = 'Start'
                        WHERE NOT EXISTS ( SELECT p2.check
                                            FROM p2p p2
                                            WHERE p2.check = checks.id
                                            AND NOT p2.state = 'Start' )
                        AND task = task_
                        AND peer = peer_checked_
                        AND p1.checkingpeer = peer_checking_
                        AND "date" = now()::date
            )
        THEN -- нет незавершенной P2P проверки, относящейся к конкретному заданию, пиру и проверяющему
            INSERT INTO checks(peer, task, "date") VALUES (peer_checked_, task_, now()::date) RETURNING id INTO check_id;
            INSERT INTO p2p("check", checkingpeer, state, "time") VALUES (check_id, peer_checking_, status_, time_::time);
        ELSE
            raise notice 'insert error % % % %', check_id, peer_checking_, status_, time_::time;
        end if;
    ELSE -- Иначе указать проверку с самым поздним (по времени) незавершенным P2P этапом
        -- Находим check_id, последней начавшейся проверки
        check_id := (SELECT checks.id
                     FROM checks
                     JOIN p2p on "check" = checks.id
                     WHERE task = task_
                       AND state = 'Start'
                       AND peer = peer_checked_
                       AND checkingpeer = peer_checking_
                       AND "date" = now()::date
                       AND time <= time_
                     ORDER BY "time" DESC
                     LIMIT 1);
        IF check_id != 0 THEN -- Если нашли, то добавляем запись в таблицу p2p с данным check_id
            INSERT INTO p2p("check", checkingpeer, state, "time")
            VALUES (check_id, peer_checking_, status_, time_::time);
        ELSE
            raise notice 'insert error % % % %', check_id, peer_checking_, status_, time_::time;
        END IF;
    END IF;
END;
$$;


-- 2) Написать процедуру добавления проверки Verter'ом
CREATE OR REPLACE PROCEDURE pr_verter_insert(IN peer_checked_ text, IN task_ varchar, IN status_ check_status,
                                             IN time_ time)
LANGUAGE plpgsql AS
$$
DECLARE
    check_id integer := 0;
BEGIN
    check_id := (SELECT c.id
                 FROM checks c
                  JOIN p2p p on p."check" = c.id
                 WHERE state = 'Success'
                   AND task = task_
                   AND peer = peer_checked_
                   AND "date" = now()::date
                   AND time <= time_
                 ORDER BY "time" DESC
                 LIMIT 1);
    IF status_ = 'Start' and check_id != 0 THEN
        IF not exists(  SELECT checks.id, v1.state
                        FROM checks
                        JOIN verter v1 on checks.id = v1."check" AND v1.state = 'Start'
                        WHERE NOT EXISTS ( SELECT v2.check
                                            FROM verter v2
                                            WHERE v2.check = checks.id
                                            AND NOT v2.state = 'Start' )
                        AND task = task_
                        AND peer = peer_checked_
                        AND "date" = now()::date)
        THEN
            INSERT INTO verter("check", state, "time") VALUES (check_id, status_, time_);
        ELSE
            raise notice 'insert error % % %', check_id, status_, time_;
        end if;
    ELSE
        IF check_id != 0 THEN
            INSERT INTO verter("check", state, "time") VALUES (check_id, status_, time_);
        ELSE
            raise notice 'insert error % % %', check_id, status_, time_;
        END IF;
    END IF;
END;
$$;




-- additional procedure for fill data in xp table
CREATE OR REPLACE PROCEDURE pr_xp_insert
(IN peer_checked_ text, IN task_ varchar, IN starttime_ time, IN percentxp_ integer)
LANGUAGE plpgsql AS $$
DECLARE 
check_id integer := 0;
xp_ integer := (SELECT maxxp FROM tasks WHERE title = task_) * percentxp_ / 100;
BEGIN
    check_id := (SELECT c.id FROM checks c
                 JOIN p2p p on c.id = p."check"
                 WHERE p.state = 'Start' AND c.peer = peer_checked_ AND c.task = task_ AND p.time = starttime_);
    IF check_id != 0 THEN
        INSERT INTO xp("check", "xpamount") VALUES (check_id, xp_);
    END IF;
END; $$;



-- 3) Написать триггер: после добавления записи со статутом "начало" в таблицу P2P, изменить соответствующую запись в таблице TransferredPoints
DROP TRIGGER IF EXISTS trg_TransferredPoints_insert_audit ON P2P;
DROP FUNCTION IF EXISTS pr_trg_TransferredPoints_insert_audit() cascade;

CREATE OR REPLACE FUNCTION pr_trg_TransferredPoints_insert_audit() RETURNS trigger AS
$$
DECLARE
    checked_name text;
BEGIN
    -- найти проверяемого(checked) через таблицу p2p
    checked_name := (select peer
                     from checks
                     where id = new.check);
    IF TG_OP = 'INSERT' AND new.state = 'Start' THEN
        -- проверка была ли раньше запись о этой паре проверяющий+проверяемый
        IF exists(SELECT *
                  from transferredpoints
                  WHERE checkingpeer = new.checkingpeer
                    and checkedpeer = checked_name) THEN
            -- прибавляем +1 к pointsamount имеющийся записи
            UPDATE transferredpoints
            SET pointsamount=pointsamount + 1
            WHERE checkingpeer = new.checkingpeer
              and checkedpeer = checked_name;
        ELSE
            -- если не было записи с такими пирами в transferredpoints - добавляем новую запись в таблицу
            INSERT INTO transferredpoints(checkingpeer, checkedpeer, pointsamount)
            VALUES (new.checkingpeer, checked_name, 1);
        END IF;
    END IF;
    RETURN new;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_TransferredPoints_insert_audit
    AFTER INSERT
    ON P2P
    FOR EACH ROW
EXECUTE PROCEDURE pr_trg_TransferredPoints_insert_audit();

-- 4) Написать триггер: перед добавлением записи в таблицу XP, проверить корректность добавляемой записи
DROP TRIGGER IF EXISTS trg_XP_insert_audit ON XP;
DROP FUNCTION IF EXISTS fnc_trg_XP_insert_audit() cascade;

CREATE OR REPLACE FUNCTION fnc_trg_XP_insert_audit() RETURNS trigger AS
$$
BEGIN
    -- Количество XP не превышает максимальное доступное для проверяемой задачи
    IF new.xpamount <= (select maxxp
                        from checks
                                 join tasks t on t.title = checks.task
                            and checks.id = new."check")
    -- Поле Check ссылается на успешную проверку
        AND exists(select p."check"
                   from p2p p
                    left join verter v on p."check" = v."check"
                    where p.state = 'Success' and (v.state = 'Success' OR v.state IS NULL)
                        and p."check"=new."check")
    THEN
        RETURN new;
    ELSE
        raise notice 'insert error % % %', new."check", new.id, new.xpamount;
        RETURN NULL;
    END IF;
END;
$$
    LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_XP_insert_audit
    BEFORE INSERT
    ON XP
    FOR EACH ROW
EXECUTE PROCEDURE fnc_trg_XP_insert_audit();
