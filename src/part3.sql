--------------------- BEFORE TASKS NEED FILL DATA INTO TIMETRACKING TABLE ---------------------

-- -- PLEASE RUN timetracking_fill_data.sql


------------------------ ADDITIONAL VIEW, FUNCTION, TRIGGER ---------------------------------

-- вспомогательная MATERIALIZED VIEW mv_checks, так как в разных tasks присутсвует
-- одинаковый запрос к одним и тем же данным

DROP MATERIALIZED VIEW IF EXISTS mv_checks;
CREATE MATERIALIZED VIEW mv_checks AS
    SELECT  ch.id,
            ch.date,
            ch.peer,
            ch.task,
            CASE WHEN v.state = 'Failure' OR p2p.state = 'Failure'
                 THEN 'Failure'::check_status
                 ELSE 'Success'::check_status
                 END AS state
    FROM checks ch
    JOIN p2p ON p2p.check = ch.id AND NOT p2p.state = 'Start'
    LEFT JOIN verter v ON v.check = ch.id AND NOT v.state = 'Start';

-- функция обновления VIEW mv_checks
CREATE OR REPLACE FUNCTION tg_refresh_mv_checks()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW mv_checks;
    RETURN NULL;
END;
$$;

-- триггер обновления VIEW mv_checks, если обновятся данные в p2p table
CREATE OR REPLACE TRIGGER tg_refresh_mv_checks AFTER INSERT OR UPDATE OR DELETE
ON p2p
FOR EACH STATEMENT EXECUTE PROCEDURE tg_refresh_mv_checks();

-- триггер обновления VIEW mv_checks, если обновятся данные в verter table
CREATE OR REPLACE TRIGGER tg_refresh_mv_checks AFTER INSERT OR UPDATE OR DELETE
ON verter
FOR EACH STATEMENT EXECUTE PROCEDURE tg_refresh_mv_checks();

------------------------------------ TASKS ---------------------------------------------

-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
CREATE OR REPLACE FUNCTION fnc_task1()
RETURNS TABLE ("Peer1" varchar, "Peer2" varchar, "PointsAmount" integer) AS $$
    WITH tmp AS (
        SELECT tp.checkingPeer, tp.checkedPeer, tp.pointsamount FROM TransferredPoints tp
        JOIN TransferredPoints t2 ON t2.checkingPeer = tp.checkedPeer
        AND t2.checkedPeer = tp.checkingPeer AND tp.id < t2.id
    )
    (SELECT checkingPeer, checkedPeer, sum(res.pointsamount)
     FROM (SELECT f.checkingPeer, f.checkedPeer, f.pointsamount FROM TransferredPoints f
            UNION
            SELECT t.checkedPeer, t.checkingPeer, -t.pointsamount FROM tmp t) AS res
    GROUP BY 1, 2)
    EXCEPT
    SELECT t.checkingPeer, t.checkedPeer, t.pointsamount FROM tmp t
    ORDER BY 1;
$$ LANGUAGE sql;

SELECT * FROM fnc_task1();

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
CREATE OR REPLACE FUNCTION fnc_task2()
RETURNS TABLE ("Peer" varchar, "Task" varchar, "XP" integer) AS $$
    SELECT ch.peer, ch.task, xp.xpamount FROM mv_checks ch
    JOIN xp ON xp.check = ch.id
    WHERE ch.state = 'Success'
    ORDER BY 1, 3 DESC;
$$ LANGUAGE sql;

SELECT * FROM fnc_task2();

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
CREATE OR REPLACE FUNCTION fnc_task3(IN pdate date)
RETURNS TABLE (peer VARCHAR) AS $$
    SELECT peer FROM TimeTracking
    WHERE date = pdate AND state = '1'
    GROUP BY peer
    HAVING count(state) = 1;
$$ LANGUAGE sql;

SELECT * FROM fnc_task3('2022-05-12');

-- 4) Найти процент успешных и неуспешных проверок за всё время
CREATE OR REPLACE PROCEDURE pr_task4(IN ref refcursor) AS $$
DECLARE
    Success integer := (SELECT count(*) FROM mv_checks ch
                        WHERE ch.state = 'Success');

    Failure integer := (SELECT count(*) FROM mv_checks ch
                        WHERE ch.state = 'Failure');
BEGIN
OPEN ref FOR
SELECT CASE
            WHEN (Success + Failure) != 0 THEN
                round(Success * 100 / (Success + Failure)::numeric)
            ELSE 0
       END AS "SuccessfulChecks",
       CASE
            WHEN (Success + Failure) != 0 THEN
                round(Failure * 100 / (Success + Failure)::numeric)
            ELSE 0
       END AS "UnsuccessfulChecks";
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task4('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_get_peer_points_change(IN peername text)
RETURNS integer AS $$
BEGIN
-- кол-во заработанных поинтов если пир есть в столбце checkingpeer
RETURN (SELECT COALESCE((SELECT(sum(pointsamount)) -- COALESCE если не нашлось такого пира, и после sum получился результат NULL
                        FROM transferredpoints
                        WHERE checkingpeer=peername
                        GROUP BY checkingpeer), 0)
                       +
                       -- и кол-во потраченных поинтов если пир есть в столбце checkedpeer
                       (SELECT COALESCE((SELECT(sum(pointsamount)*-1) -- меняем знак
                                                FROM transferredpoints
                                                WHERE checkedpeer=peername
                                                GROUP BY checkedpeer), 0))
);-- в результате получается сумма, если отрицательная то пир проверялся больше чем проверял
END;$$
LANGUAGE plpgsql;

CREATE or replace PROCEDURE pr_task5(IN ref refcursor)
LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
    SELECT name AS Peer, fnc_get_peer_points_change(name) AS PointsChange
    FROM (SELECT checkingpeer AS name
            FROM transferredpoints
            UNION DISTINCT
            SELECT checkedpeer AS name
            FROM transferredpoints ) AS names
    ORDER BY Peer;
END;$$;

BEGIN;
    CALL pr_task5('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
DROP PROCEDURE IF EXISTS pr_task6(IN ref refcursor);
CREATE OR REPLACE PROCEDURE pr_task6(IN ref refcursor)
LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
SELECT name AS Peer, fnc_get_peer_points_change(name) AS PointsChange
              FROM (SELECT "Peer1" AS name
                    FROM fnc_task1()
                    UNION DISTINCT
                    SELECT "Peer2" AS name
                    FROM fnc_task1()
                    ) AS names
              ORDER BY PointsChange DESC;
END;$$;

BEGIN;
    CALL pr_task6('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 7) Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE FUNCTION fnc_get_max_count_task(IN date_ date)
RETURNS integer AS $$
BEGIN
RETURN (WITH checksinday AS(SELECT c.date, c.task, count(*) AS count_
                            FROM checks c
                            GROUP BY 1, 2
                            ORDER BY 1) SELECT max(cd.count_)
                                        FROM checksinday cd
                                        WHERE cd.date=date_

);
END;$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE pr_task7(IN ref refcursor)
LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
    SELECT to_char(date, 'DD.MM.YYYY') AS day, task
    FROM(SELECT c.date, c.task, count(*) AS count_
        FROM checks c
        GROUP BY 1, 2
        ORDER BY 1) AS t
    WHERE count_=fnc_get_max_count_task(date)
    ORDER BY 1, 2;
END;$$;

BEGIN;
    CALL pr_task7('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 8) Определить длительность последней P2P проверки
CREATE OR REPLACE PROCEDURE pr_task8(IN ref refcursor)
LANGUAGE plpgsql AS $$
DECLARE
-- найти check последней проверки где есть старт и завершение
check_ integer := (SELECT p1."check"
                    FROM p2p p1
                    JOIN p2p p2 ON p2."check"=p1."check" AND p2.state='Start'
                    WHERE p1.state!='Start'
                    ORDER BY p1.time DESC
                    LIMIT 1);

endtime time := (SELECT time FROM p2p
                WHERE "check"=check_ AND state!='Start'
                ORDER BY time DESC
                LIMIT 1);
starttime time := (SELECT time FROM p2p
                    WHERE "check"=check_ AND state='Start'
                    ORDER BY time DESC
                    LIMIT 1
                    );
BEGIN
    OPEN ref FOR
    SELECT endtime-starttime AS last_check_duration;
END;
$$;

BEGIN;
    CALL pr_task8('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE PROCEDURE pr_task9(IN ref refcursor, IN blockname VARCHAR) AS $$
BEGIN
OPEN ref FOR
with cte AS(
    SELECT c.peer, task, c.date AS day, row_number() OVER (PARTITION BY peer) AS completed_task_counter
    FROM p2p p1
    JOIN checks c ON c.id = p1."check"
    left JOIN verter v1 ON c.id = v1."check" and (v1.state='Success' OR v1.state IS NULL)
    WHERE c.task LIKE (blockname||'%') and p1.state='Success'
    and p1."check" in (SELECT p1."check"
                        FROM p2p p2
                        JOIN checks c ON c.id = p2."check"
                        left JOIN verter v2 ON v2."check" = v1."check" and (v1.state='Success' OR v1.state IS NULL)
                        WHERE p2.state = 'Start' and p2."check" = p1."check")
    GROUP BY c.peer, task, date
)
SELECT peer, day
FROM cte
WHERE completed_task_counter = (SELECT count(*)
                                 FROM tasks
                                 WHERE tasks.title LIKE (blockname||'%'))
ORDER BY day DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task9('cursor_name', 'CPP');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
CREATE OR REPLACE PROCEDURE pr_task10 (IN ref refcursor)
AS $$
BEGIN
OPEN ref FOR
    WITH w_tmp1 AS (SELECT p.nickname AS peer, f.peer2 AS friend, r.recommendedpeer AS recommendedpeer
    FROM peers p
    INNER JOIN friends f ON p.nickname = f.peer1
    INNER JOIN recommendations r ON f.peer2 = r.peer AND p.nickname != r.recommendedpeer
    ORDER BY 1,2),
    w_tmp2 AS (
    SELECT peer, recommendedpeer, count(recommendedpeer) AS count_of_recommends
    FROM w_tmp1
    GROUP BY 1,2
    ORDER BY 1,2),
    w_tmp3 AS (
    SELECT peer, recommendedpeer, count_of_recommends, ROW_NUMBER() OVER (PARTITION BY peer ORDER BY count_of_recommends DESC) AS num_of_row_for_each_peer
    FROM w_tmp2
    )
    SELECT peer, recommendedpeer
    FROM w_tmp3
    WHERE num_of_row_for_each_peer = 1;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task10('cursor_name');
    FETCH ALL FROM "cursor_name";
END;

-- 11) Определить процент пиров, которые:Приступили к блоку 1,Приступили к блоку 2, Приступили к обоим, Не приступили ни к одному
CREATE OR REPLACE FUNCTION PeersStartedBlock("block" varchar)
    RETURNS table("Peer" varchar) AS $$
    SELECT DISTINCT ch.peer
    FROM Checks ch
    WHERE ch.task LIKE "block" || '%'
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_task11(IN ref refcursor, IN block1 VARCHAR, IN block2 VARCHAR)
AS $$
DECLARE
        StartedBlock1 INTEGER := (SELECT count(*) FROM PeersStartedBlock("block1"));
        StartedBlock2 INTEGER := (SELECT count(*) FROM PeersStartedBlock("block2"));
        StartedBothBlocks INTEGER := (SELECT count(*) FROM (
                                                                SELECT "Peer"  FROM PeersStartedBlock("block1")
                                                                INTERSECT
                                                                SELECT "Peer"  FROM PeersStartedBlock("block2")
                                                            ) AS res);
        DidntStartAnyBlock INTEGER := (SELECT count(*) FROM (
                                                                SELECT nickname FROM peers
                                                                EXCEPT
                                                                (SELECT "Peer" FROM PeersStartedBlock("block1")
                                                                UNION
                                                                SELECT "Peer" FROM PeersStartedBlock("block2"))
                                                            ) AS res1);
        AllPeers INTEGER := (SELECT count(*) FROM peers);

BEGIN
OPEN ref FOR
SELECT StartedBlock1 * 100 / AllPeers AS StartedBlock1,
        StartedBlock2 * 100 / AllPeers AS StartedBlock2,
        StartedBothBlocks * 100 / AllPeers AS StartedBothBlocks,
        DidntStartAnyBlock * 100 / AllPeers AS DidntStartAnyBlock;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task11('cursor_name', 'D0', 'C');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 12) Определить N пиров с наибольшим числом друзей
CREATE OR REPLACE PROCEDURE pr_task12(IN ref refcursor, IN fLIMIT integer) AS $$
BEGIN
OPEN ref FOR
    SELECT peer1 AS "Peer", count(*) AS "FriendsCount"
    FROM (SELECT peer1 FROM friends
            UNION ALL
            SELECT peer2 FROM friends) AS res
    GROUP BY peer1
    ORDER BY "FriendsCount" DESC
    LIMIT fLIMIT;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task12('cursor_name', 7);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
UPDATE peers
SET birthday = '1994-11-10'
WHERE nickname = 'duck' OR nickname = 'jersey' OR nickname = 'class';

CREATE OR REPLACE FUNCTION fnc_ChecksCountOnBirthday(fstate check_status)
    RETURNS integer AS $$
        SELECT count(*) AS count_ FROM mv_checks ch
        JOIN peers p ON p.nickname = ch.peer
        WHERE substring(ch.date::text, 5) = substring(p.birthday::text, 5) AND ch.state = fstate
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_task13(IN ref refcursor) AS $$
DECLARE
    s integer := (SELECT * FROM fnc_ChecksCountOnBirthday('Success'));
    f integer := (SELECT * FROM fnc_ChecksCountOnBirthday('Failure'));
BEGIN
    OPEN ref FOR
        SELECT CASE
                    WHEN (s + f) != 0 THEN round(s * 100 / (s + f)::numeric)
                    ELSE 0
                END AS "SuccessfulChecks",
               CASE
                    WHEN (s + f) != 0 THEN round(f * 100 / (s + f)::numeric)
                    ELSE 0
                END AS "UnsuccessfulChecks";

END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task13('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

UPDATE peers
SET birthday = '1999-09-18'
WHERE nickname = 'duck';

UPDATE peers
SET birthday = '1984-04-19'
WHERE nickname = 'jersey';

UPDATE peers
SET birthday = '1993-02-21'
WHERE nickname = 'class';


-- 14) Определить кол-во XP, полученное в сумме каждым пиром
CREATE OR REPLACE PROCEDURE pr_task14 (IN ref refcursor)
AS $$
BEGIN
OPEN ref FOR
    SELECT peer, sum(maxpoint) AS xp
    FROM (SELECT p.nickname AS peer, t.title AS project, max(xp.xpamount) AS maxpoint
        FROM xp INNER JOIN Checks ch ON ch.id = xp."check"
        INNER JOIN Peers p ON p.nickname = ch.peer
        INNER JOIN Tasks t ON ch.task = t.title
        GROUP BY 1,2
        ORDER BY 1,2) tmp
    GROUP BY peer
    ORDER BY 2 DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task14('cursor_name');
    FETCH ALL FROM "cursor_name";
END;

-- 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
CREATE OR REPLACE FUNCTION fnc_PeerCompletedTasks(fpeer text, ftask text)
RETURNS bool AS $$
    SELECT EXISTS (SELECT ch.peer
                    FROM mv_checks ch
                    WHERE ch.peer = fpeer AND ch.task = ftask AND ch.state = 'Success')
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_task15(IN ref refcursor, IN firstT text, IN secondT text, IN third text) AS $$
BEGIN
OPEN ref FOR
    SELECT DISTINCT p.nickname FROM peers p
    JOIN checks ch ON ch.peer = p.nickname
    WHERE fnc_PeerCompletedTasks(p.nickname, firstT)
        AND fnc_PeerCompletedTasks(p.nickname, secondT)
        AND NOT fnc_PeerCompletedTasks(p.nickname, third);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task15('cursor_name', 'C4_Math', 'C5_Decimal', 'DO1_Linux');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE FUNCTION recursiveCountParent(tasks_ text)
RETURNS integer AS $$
    WITH RECURSIVE test AS (
        SELECT title, parentTask, 0 AS level
        FROM tasks
        WHERE title = tasks_
        UNION ALL
        SELECT t.title, t.parentTask, test.level + 1
        FROM tasks t
        JOIN test ON t.title = test.parentTask
    )
    SELECT max(level) FROM test
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_task16(IN ref refcursor) AS $$
BEGIN
OPEN ref FOR
    SELECT t.title, recursiveCountParent(t.title) FROM tasks t;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task16('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 17) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
CREATE OR REPLACE PROCEDURE pr_task17(IN ref refcursor, IN N integer) AS $$
BEGIN
OPEN ref FOR
    WITH AllGroup AS
    (
        SELECT "id",
                "task",
                "state",
                "date",
                SUM(CASE WHEN "state" = prev THEN 0 ELSE 1 END) OVER(ORDER BY "id") AS grp
        FROM (SELECT *, LAG("state") OVER(ORDER BY "id") AS prev
                FROM mv_checks) AS res
    )

    SELECT "date"::date
    FROM ( SELECT "date", count(*) AS CountChecks
            FROM AllGroup ag
            JOIN xp ON xp.check = ag.id
            JOIN tasks t ON t.title = ag."task"
            WHERE ag."state" = 'Success' AND xp.xpAmount > t.maxXP * 0.8
            GROUP BY grp, "date"
        ) AS res
    GROUP BY "date"
    HAVING max(CountChecks) >= N;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task17('cursor_name', 8);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 18) Определить пира с наибольшим числом выполненных заданий
CREATE OR REPLACE PROCEDURE pr_task18(IN ref refcursor) AS $$
BEGIN
OPEN ref FOR
    WITH CompletedTask AS (
        SELECT peer, count(*) AS count_ FROM mv_checks ch
        WHERE ch.state = 'Success'
        GROUP BY peer)
    SELECT ct.peer AS "Peer", count_ AS "Tasks" FROM CompletedTask ct
    WHERE ct.count_ = (SELECT max(count_) FROM CompletedTask);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task18('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 19) Определить пира с наибольшим количеством XP
CREATE OR REPLACE PROCEDURE pr_task19 (IN ref refcursor)
AS $$
BEGIN
OPEN ref FOR
    SELECT p.nickname AS Peer, sum(xpAmount) AS XP
    FROM Checks ch
    INNER JOIN XP ON ch.id = xp.check
    INNER JOIN Peers p ON p.nickname = ch.peer
    GROUP BY p.nickname
    HAVING sum(xpAmount) = (SELECT sum(xpAmount)
                            FROM Checks ch
                            INNER JOIN XP ON ch.id = xp.check
                            INNER JOIN Peers p ON p.nickname = ch.peer
                            GROUP BY p.nickname
                            ORDER BY 1 DESC LIMIT 1);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task19('cursor_name');
    FETCH ALL FROM "cursor_name";
END;

-- 20) Определить пира, который провел сегодня в кампусе больше всего времени
CREATE OR REPLACE PROCEDURE pr_task20 (IN ref refcursor)
AS $$
BEGIN
OPEN ref FOR
    WITH intime AS(
    SELECT id,peer, "time"
    FROM TimeTracking
    WHERE state = '1' AND "date" = now()::date),
    outtime AS(
    SELECT id,peer, "time"
    FROM TimeTracking
    WHERE state = '2' AND "date" = now()::date),
    JOINtable AS (
    SELECT DISTINCT ON (i.id) i.id AS id1, i.peer AS peer1, i."time" AS time1, o.id AS id2, o.peer AS peer2, o."time" AS time2
    FROM intime i INNER JOIN outtime o ON i.peer = o.peer AND i."time" < o."time"
    ORDER BY 1,2,3,6),
    peerandmaxtime AS (
    SELECT peer1 AS peer
    FROM JOINtable
    GROUP BY peer1
    HAVING (sum(time2 - time1)::time) = (SELECT (sum(time2 - time1)::time) AS time3
                                        FROM JOINtable
                                        GROUP BY peer1
                                        ORDER BY 1 DESC LIMIT 1)
    )SELECT * FROM peerandmaxtime;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task20('cursor_name');
    FETCH ALL FROM "cursor_name";
END;

-- 21) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
CREATE OR REPLACE PROCEDURE pr_task21(IN ref refcursor, time_ time, mincount integer) AS $$
BEGIN
OPEN ref FOR
    SELECT peer
    FROM (SELECT peer, "date", min("time")
            FROM timetracking
            WHERE "state" = '1' AND "time" < time_
            GROUP BY 1, 2
            ORDER BY 2) AS res
    GROUP BY peer
    HAVING count(*) >= mincount;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task21('cursor_name', '13:00:00', 2);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 22) Определить пиров, выходивших за последние N дней из кампуса больше M раз
CREATE OR REPLACE PROCEDURE pr_task22(IN ref refcursor, Ndays integer, mincount integer) AS $$
BEGIN
OPEN ref FOR
    SELECT peer
    FROM (SELECT peer, "date", count(*) - 1 AS count_
            FROM timetracking
            WHERE "state" = '1' AND "date" > (now()::date - Ndays)
            GROUP BY 1, 2
            ORDER BY 2) AS res
    GROUP BY peer
    HAVING sum(count_) > mincount;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task22('cursor_name', 2, 1);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 23) Определить пира, который пришел сегодня последним
CREATE OR REPLACE PROCEDURE pr_task23(IN ref refcursor) AS $$
BEGIN
OPEN ref FOR
    SELECT peer
    FROM (SELECT peer, "date", min("time")
            FROM timetracking
            WHERE "state" = '1' AND "date" = now()::date
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT 1) AS res;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task23('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 24) Определить пиров, которые выходили вчера из кампуса больше чем на N минут
CREATE OR REPLACE PROCEDURE pr_task24(IN ref refcursor, N integer) AS $$
BEGIN
OPEN ref FOR
    WITH in_ AS (
        SELECT id, peer, "date", "time"
        FROM timetracking tt
        WHERE tt.state = '1' AND "date" = current_date - 1
        AND NOT tt.time = ( SELECT min("time")
                            FROM timetracking tt2
                            WHERE tt2.date = tt.date AND tt2.peer = tt.peer)
        ORDER BY 2, 4
    ), out_  AS
    (SELECT id, peer, "date", "time"
    FROM timetracking tt
    WHERE tt.state = '2' AND "date" = current_date - 1
    AND NOT tt.time = ( SELECT max("time")
                        FROM timetracking tt2
                        WHERE tt2.date = tt.date AND tt2.peer = tt.peer)
        ORDER BY 2, 4
    ), InAndOut AS
    ( SELECT DISTINCT ON (in_.id) in_.id, in_.peer, out_.time AS out_, in_.time AS in_
        FROM in_
        JOIN out_ ON in_.peer = out_.peer AND in_.date = out_.date
        WHERE out_.time < in_.time)

    SELECT peer FROM InAndOut
    GROUP BY peer
    HAVING sum(InAndOut.in_ - InAndOut.out_) > make_time(N / 60, N - N / 60 * 60, 0.);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task24('cursor_name', 120);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 25) Определить для каждого месяца процент ранних входов
CREATE OR REPLACE FUNCTION countVisit(peer_ text, mintime time DEFAULT '24:00:00')
    RETURNS integer AS $$
    SELECT count(*) FROM ( SELECT tt.peer, tt.date
                            FROM timetracking tt
                            WHERE tt.peer = peer_
                            GROUP BY 1, 2
                            HAVING min("time") < mintime) AS res
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_task25(IN ref refcursor) AS $$
BEGIN
OPEN ref FOR
    WITH Months AS (
        SELECT gs::date AS "Month"
        FROM generate_series('2000-01-31', '2000-12-31', interval '1 month') AS gs
    ), VisitsInMonthOfBirth AS
    (SELECT TO_CHAR("Month"::date, 'Month') AS "Month",
    sum(countVisit(p.nickname::text)) AS "AllVisits",
    sum(countVisit(p.nickname::text, '12:00:00')) AS "EarlyVisits"
    FROM Months
    LEFT JOIN peers p ON EXTRACT(MONTH FROM "Month") = EXTRACT(MONTH FROM p.birthday::date)
    GROUP BY "Month")

    SELECT "Month", CASE WHEN "AllVisits" = 0
                    THEN 0
                    ELSE ("EarlyVisits" * 100 / "AllVisits")
                    END AS "EarlyEntries"
    FROM VisitsInMonthOfBirth
    ORDER BY to_date("Month", 'Mon');
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task25('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;
