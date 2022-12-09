------------------------------------ BEFOR RUN TEST TASKS ---------------------------------------------

-- delete from transferredpoints;
-- delete from p2p;
-- delete from verter;
-- delete from xp;
-- delete from checks;

-- ALTER SEQUENCE transferredpoints_id_seq RESTART WITH 1;
-- ALTER SEQUENCE p2p_id_seq RESTART WITH 1;
-- ALTER SEQUENCE verter_id_seq RESTART WITH 1;
-- ALTER SEQUENCE xp_id_seq RESTART WITH 1;
-- ALTER SEQUENCE checks_id_seq RESTART WITH 1;

-- -- PLEASE RUN p2p_fill_data.sql && timetracking_fill_data.sql

------------------------------------ ADDITIONAL VIEW, FUNCTION, TRIGGER ---------------------------------------------

-- вспомогательная MATERIALIZED VIEW mv_checks, так как много одинаковых запросов из разных tasks
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
RETURNS trigger LANGUAGE plpgsql AS $$
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
CREATE OR REPLACE FUNCTION fnc_part1()
RETURNS TABLE ("Peer1" varchar, "Peer2" varchar, "PointsAmount" integer) AS $$
    WITH tmp as (
        SELECT tp.checkingPeer, tp.checkedPeer, tp.pointsamount FROM TransferredPoints tp
        JOIN TransferredPoints t2 ON t2.checkingPeer = tp.checkedPeer 
        AND t2.checkedPeer = tp.checkingPeer AND tp.id < t2.id
    )
    (SELECT checkingPeer, checkedPeer, sum(res.pointsamount) FROM 
    (
        SELECT f.checkingPeer, f.checkedPeer, f.pointsamount FROM TransferredPoints f
        UNION
        SELECT t.checkedPeer, t.checkingPeer, -t.pointsamount FROM tmp t
    ) AS res
    GROUP BY 1, 2)
    EXCEPT
    SELECT t.checkingPeer, t.checkedPeer, t.pointsamount FROM tmp t;

$$ LANGUAGE sql;

SELECT * FROM fnc_part1()

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
CREATE OR REPLACE FUNCTION fnc_part2()
RETURNS TABLE ("Peer" varchar, "Task" varchar, "XP" integer) AS $$
    SELECT ch.peer, ch.task, xp.xpamount FROM mv_checks ch
    JOIN xp ON xp.check = ch.id
    WHERE ch.state = 'Success'
    ORDER BY 1, 3 DESC
$$ LANGUAGE sql;

SELECT * FROM fnc_part2()

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
CREATE OR REPLACE FUNCTION fnc_part3(pdate date) 
    RETURNS TABLE (peer VARCHAR) AS $$
        SELECT peer FROM TimeTracking
        WHERE date = pdate AND state = '1'
        GROUP BY peer
        HAVING count(state) = 1
$$ LANGUAGE sql;

SELECT * FROM fnc_part3('2022-05-12');

-- 4) Найти процент успешных и неуспешных проверок за всё время
DROP PROCEDURE IF EXISTS pr_part4(IN ref refcursor);
CREATE OR REPLACE PROCEDURE pr_part4(IN ref refcursor) AS $$
    DECLARE
    Success integer := (SELECT count(*) FROM mv_checks ch
                        WHERE ch.state = 'Success');

    Failure integer := (SELECT count(*) FROM mv_checks ch
                        WHERE ch.state = 'Failure');               
    BEGIN
    OPEN ref FOR
        SELECT  round(Success * 100 / (Success + Failure)::numeric) AS "SuccessfulChecks", 
                round(Failure * 100 / (Success + Failure)::numeric) AS "UnsuccessfulChecks";
END; 
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part4('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
CREATE OR REPLACE FUNCTION fnc_get_peer_points_change(IN peername text)
RETURNS integer AS $$
BEGIN
-- кол-во заработанных поинтов если пир есть в столбце checkingpeer
return (select COALESCE((select(sum(pointsamount)) -- COALESCE если не нашлось такого пира, и после sum получился результат NULL
                        from transferredpoints
                        where checkingpeer=peername
                        group by checkingpeer), 0)
                       +
                       -- и кол-во потраченных поинтов если пир есть в столбце checkedpeer
                       (select COALESCE((select(sum(pointsamount)*-1) -- меняем знак
                                                from transferredpoints
                                                where checkedpeer=peername
                                                group by checkedpeer), 0))
);-- в результате получается сумма, если отрицательная то пир проверялся больше чем проверял
END;$$
LANGUAGE plpgsql;

DROP PROCEDURE IF EXISTS pr_PointsChanges(IN ref refcursor);
CREATE or replace PROCEDURE pr_PointsChanges(IN ref refcursor)
LANGUAGE plpgsql as $$
BEGIN
OPEN ref FOR
select name as Peer, fnc_get_peer_points_change(name) as PointsChange
                  from (select checkingpeer as name
                        from transferredpoints
                        union distinct
                        select checkedpeer as name
                        from transferredpoints ) as names
                  order by PointsChange DESC;
END; $$;

BEGIN;
    CALL pr_PointsChanges('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 6) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
DROP PROCEDURE IF EXISTS pr_PointsChanges2(IN ref refcursor);
CREATE OR REPLACE PROCEDURE pr_PointsChanges2(IN ref refcursor)
LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
select name as Peer, fnc_get_peer_points_change(name) as PointsChange
              from (select "Peer1" as name
                    from fnc_part1()
                    union distinct
                    select "Peer2" as name
                    from fnc_part1()
                    ) as names
              order by PointsChange DESC;
END;$$;

BEGIN;
    CALL pr_PointsChanges2('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 7) Определить самое часто проверяемое задание за каждый день
CREATE OR REPLACE FUNCTION fnc_get_max_count_task(IN date_ date)
RETURNS integer AS $$
BEGIN
return (WITH checksinday AS (
select c.date, c.task, count(*) as count_
from checks c
group by 1, 2
order by 1
)
select max(cd.count_)
from checksinday cd
where cd.date=date_);
END;$$
LANGUAGE plpgsql;


CREATE OR REPLACE PROCEDURE pr_most_checked_tack_each_day(IN ref refcursor)
LANGUAGE plpgsql AS $$
BEGIN
OPEN ref FOR
    select to_char(date, 'DD.MM.YYYY') as day, task
    from(select c.date, c.task, count(*) as count_
        from checks c
        group by 1, 2
        order by 1) as t
    where count_=fnc_get_max_count_task(date);
END;$$;

BEGIN;
    CALL pr_most_checked_tack_each_day('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 8) Определить длительность последней P2P проверки
CREATE OR REPLACE PROCEDURE pc_DurationLastCheck(IN ref refcursor)
LANGUAGE plpgsql AS $$
declare
-- найти check последней проверки где есть старт и завершение
check_ integer := (
    select p1."check"
    from p2p p1
    join p2p p2 on p2."check"=p1."check" and p2.state='Start'
    where p1.state!='Start'
    order by p1.time desc
    limit 1);

endtime time := (select time from p2p
                where "check"=check_ and state!='Start'
                order by time desc
                limit 1);
starttime time := (select time from p2p
                where "check"=check_ and state='Start'
                order by time desc
                limit 1
                );
BEGIN
    OPEN ref FOR
    select endtime-starttime as check_duration;
END;
$$;

BEGIN;
    CALL pc_DurationLastCheck('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 9) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
CREATE OR REPLACE PROCEDURE pr_CompletedBlock(IN ref refcursor, IN blockname VARCHAR) AS $$
BEGIN
OPEN ref FOR
with completedBlockRaw as (
select c.peer, case -- получить кол-во выполненых задач пира
                    when count(distinct(select c.task
                                        from p2p p2
                                        join checks c on c.id = p2."check"
                                        where p2.state = 'Start' and p2."check" = p1."check"))
                        = -- и сравнить с кол-вом всех задач в блоке
                        (select count(*)
                         from tasks
                         where tasks.title like (blockname||'%'))
                    then c.date
                end as day
        from p2p p1
    join checks c on c.id = p1."check"
    where c.task like (blockname||'%') and state='Success'
    group by c.peer, c.date)
select cbr.peer, cbr.day
from completedBlockRaw cbr
where cbr.day is not null
order by day desc ;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_CompletedBlock('cursor_name', 'CPP');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 10) Определить, к какому пиру стоит идти на проверку каждому обучающемуся

DROP PROCEDURE IF EXISTS pr_task10;

CREATE OR REPLACE PROCEDURE pr_task10 (IN ref refcursor)
AS $$
BEGIN
    OPEN ref FOR
    WITH w_tmp1 AS (SELECT p.nickname as peer, f.peer2 as friend, r.recommendedpeer as recommendedpeer
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
---------------????-------------
-- SELECT count(*) FROM PeersStartedBlock('CPP');
---------------????-------------

CREATE OR REPLACE FUNCTION PeersStartedBlock("block" varchar) 
    RETURNS table("Peer" varchar) AS $$ 
    SELECT DISTINCT ch.peer
    FROM Checks ch
    WHERE ch.task LIKE "block" || '%'
$$ LANGUAGE sql;

CREATE OR REPLACE FUNCTION part11("block1" varchar, "block2" varchar) 
    RETURNS table ("StartedBlock1" integer, 
                  "StartedBlock2" integer, 
                  "StartedBothBlocks" integer, 
                  "DidntStartAnyBlock" integer) AS $$
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
    RETURN query (SELECT StartedBlock1 * 100 / AllPeers, 
                        StartedBlock2 * 100 / AllPeers, 
                        StartedBothBlocks * 100 / AllPeers, 
                        DidntStartAnyBlock * 100 / AllPeers);
    END;
$$ LANGUAGE plpgsql;

SELECT * FROM part11('D0', 'C');

-- 12) Определить N пиров с наибольшим числом друзей
CREATE OR REPLACE PROCEDURE pr_part12(IN ref refcursor, IN flimit integer) AS $$
    BEGIN
    OPEN ref FOR
        SELECT peer1 AS "Peer", count(*) AS "FriendsCount" FROM (
            SELECT peer1 FROM friends
            UNION ALL
            SELECT peer2 FROM friends) as res
        GROUP BY peer1
        ORDER BY "FriendsCount" DESC
        LIMIT flimit;
    END; 
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part12('cursor_name', 7);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 13) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
---------------????-------------
-- UPDATE peers
-- SET birthday = '1994-11-10'
-- WHERE nickname = 'duck' OR nickname = 'jersey' OR nickname = 'class';
---------------????-------------

CREATE OR REPLACE FUNCTION fnc_ChecksCountOnBirthday(fstate check_status) 
    RETURNS integer AS $$
        SELECT count(*) AS count_ FROM mv_checks ch
        JOIN peers p ON p.nickname = ch.peer
        WHERE substring(ch.date::text, 5) = substring(p.birthday::text, 5) AND ch.state = fstate
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_part13(IN ref refcursor) AS $$
    DECLARE 
        s integer := (SELECT * FROM fnc_ChecksCountOnBirthday('Success'));
        f integer := (SELECT * FROM fnc_ChecksCountOnBirthday('Failure'));
    BEGIN
    OPEN ref FOR
        SELECT  round(s * 100 / (s + f)::numeric) AS "SuccessfulChecks", 
                round(f * 100 / (s + f)::numeric) AS "UnsuccessfulChecks";
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part13('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 14) Определить кол-во XP, полученное в сумме каждым пиром

DROP PROCEDURE IF EXISTS pr_task14(IN ref refcursor);

CREATE OR REPLACE PROCEDURE pr_task14 (IN ref refcursor)
AS $$
BEGIN
    OPEN ref FOR
    SELECT peer, sum(maxpoint) as xp
    FROM (SELECT p.nickname as peer, t.title as project, max(xp.xpamount) as maxpoint
        FROM xp INNER JOIN Checks ch ON ch.id = xp."check"
        INNER JOIN Peers p ON p.nickname = ch.peer
        INNER JOIN Tasks t ON ch.task = t.title
        GROUP by 1,2
        ORDER by 1,2) tmp
    GROUP BY peer;
    
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task14('cursor_name');
    FETCH ALL FROM "cursor_name";
END;


-- 15) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
---------------????-------------
-- SELECT * FROM fnc_PeerCompletedTasks('class', 'DO1_Linux');
---------------????-------------

CREATE OR REPLACE FUNCTION fnc_PeerCompletedTasks(fpeer text, ftask text) 
    RETURNS bool AS $$ 
    SELECT EXISTS (
                    SELECT ch.peer 
                    FROM mv_checks ch
                    WHERE ch.peer = fpeer AND ch.task = ftask AND ch.state = 'Success'
                  )
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_part15
(IN ref refcursor, IN firstT text, IN secondT text, IN third text) AS $$
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
    CALL pr_part15('cursor_name', 'C4_Math', 'C5_Decimal', 'DO1_Linux');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 16) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
CREATE OR REPLACE FUNCTION recursiveCountParent(tasks_ text) 
    RETURNS integer AS $$ 
    WITH RECURSIVE test AS (
        SELECT title, parentTask, 0 as level
        FROM tasks 
        WHERE title = tasks_
        UNION ALL 
        SELECT t.title, t.parentTask, test.level + 1
        FROM tasks t
        JOIN test ON t.title = test.parentTask
    )
    SELECT max(level) FROM test
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_part16(IN ref refcursor) AS $$ 
    BEGIN
    OPEN ref FOR
        SELECT t.title, recursiveCountParent(t.title) FROM tasks t;
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part16('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 17) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
CREATE OR REPLACE PROCEDURE pr_part17(IN ref refcursor, IN N integer) AS $$ 
    BEGIN
    OPEN ref FOR
        WITH AllGroup AS 
        (
            SELECT "id", 
                    "task",
                    "state", 
                    "date", 
                    SUM(CASE WHEN "state" = prev THEN 0 ELSE 1 END) OVER(ORDER BY "id") AS grp
            FROM (
                    SELECT *, LAG("state") OVER(ORDER BY "id") as prev 
                    FROM mv_checks
                 ) AS res
        )

        SELECT "date"::date
        FROM (
                SELECT "date", count(*) AS CountChecks
                FROM AllGroup ag
                JOIN xp ON xp.check = ag.id
                JOIN tasks t ON t.title = ag."task"
                WHERE ag."state" = 'Success' AND xp.xpAmount > t.maxXP * 0.8
                GROUP BY grp, "date"
            ) as res
        GROUP BY "date"
        HAVING max(CountChecks) >= N;
    END; 
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part17('cursor_name', 8);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 18) Определить пира с наибольшим числом выполненных заданий
CREATE OR REPLACE PROCEDURE pr_part18(IN ref refcursor) AS $$
    BEGIN
    OPEN ref FOR
        WITH CompletedTask AS 
        (
            SELECT peer, count(*) AS count_ FROM mv_checks ch
            WHERE ch.state = 'Success'
            GROUP BY peer
        )
        SELECT ct.peer AS "Peer", count_ AS "Tasks" FROM CompletedTask ct
        WHERE ct.count_ = (SELECT max(count_) FROM CompletedTask);
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part18('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;


-- 19) Определить пира с наибольшим количеством XP

DROP PROCEDURE IF EXISTS pr_task19(IN ref refcursor);

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

DROP PROCEDURE IF EXISTS pr_task20(IN ref refcursor);

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
    jointable AS (
    SELECT DISTINCT ON (i.id) i.id as id1, i.peer as peer1, i."time" as time1, o.id as id2, o.peer as peer2, o."time" as time2
    FROM intime i INNER JOIN outtime o ON i.peer = o.peer AND i."time" < o."time"
    ORDER BY 1,2,3,6),
    peerandmaxtime AS (
    SELECT peer1 as peer
    FROM jointable
    GROUP BY peer1
    HAVING (sum(time2 - time1)::time) = (SELECT (sum(time2 - time1)::time) as time3
                                        FROM jointable
                                        GROUP BY peer1
                                        ORDER BY 1 DESC LIMIT 1)
    )
select * from peerandmaxtime;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_task20('cursor_name');
    FETCH ALL FROM "cursor_name";
END;


-- 21) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
CREATE OR REPLACE PROCEDURE pr_part21(IN ref refcursor, time_ time, mincount integer) AS $$
    BEGIN
    OPEN ref FOR
        SELECT peer FROM (
                            SELECT peer, "date", min("time") FROM timetracking
                            WHERE "state" = '1' AND "time" < time_
                            GROUP BY 1, 2
                            ORDER BY 2
                         ) AS res
        GROUP BY peer
        HAVING count(*) >= mincount;
    END;
$$ LANGUAGE plpgsql;
   
BEGIN;
    CALL pr_part21('cursor_name', '13:00:00', 2);
    FETCH ALL IN "cursor_name";
COMMIT;

---------------????-------------
-- SELECT * FROM timetracking
-- ORDER BY 2, 4;
---------------????-------------

-- 22) Определить пиров, выходивших за последние N дней из кампуса больше M раз
CREATE OR REPLACE PROCEDURE pr_part22(IN ref refcursor, Ndays integer, mincount integer) AS $$
    BEGIN
    OPEN ref FOR
        SELECT peer FROM (
                            SELECT peer, "date", count(*) - 1 AS count_ FROM timetracking
                            WHERE "state" = '1' AND "date" > (now()::date - Ndays)
                            GROUP BY 1, 2
                            ORDER BY 2
                         ) AS res
        GROUP BY peer
        HAVING sum(count_) > mincount;
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part22('cursor_name', 2, 1);
    FETCH ALL IN "cursor_name";
COMMIT;

-- 23) Определить пира, который пришел сегодня последним
CREATE OR REPLACE PROCEDURE pr_part23(IN ref refcursor) AS $$
    BEGIN
    OPEN ref FOR
        SELECT peer FROM (
                            SELECT peer, "date", min("time") FROM timetracking
                            WHERE "state" = '1' AND "date" = now()::date
                            GROUP BY 1, 2
                            ORDER BY 3 DESC
                            LIMIT 1
                         ) AS res;
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part23('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;

-- 24) Определить пиров, которые выходили вчера из кампуса больше чем на N минут
CREATE OR REPLACE PROCEDURE pr_part24(IN ref refcursor, N integer) AS $$
    BEGIN
    OPEN ref FOR
        WITH in_ AS 
        (
            SELECT id, peer, "date", "time" FROM timetracking tt
            WHERE tt.state = '1' AND "date" = current_date - 1 
            AND NOT tt.time = (
                                SELECT min("time") 
                                FROM timetracking tt2 
                                WHERE tt2.date = tt.date AND tt2.peer = tt.peer
                              )
            ORDER BY 2, 4
        ), out_  AS 
        (
            SELECT id, peer, "date", "time" FROM timetracking tt
            WHERE tt.state = '2' AND "date" = current_date - 1 
            AND NOT tt.time = (
                                SELECT max("time") 
                                FROM timetracking tt2 
                                WHERE tt2.date = tt.date AND tt2.peer = tt.peer
                              )
            ORDER BY 2, 4
        ), InAndOut AS 
        (
            SELECT DISTINCT ON (in_.id) in_.id, in_.peer, out_.time as out_, in_.time as in_
            FROM in_
            JOIN out_ ON in_.peer = out_.peer AND in_.date = out_.date
            WHERE out_.time < in_.time
        )

        SELECT peer FROM InAndOut 
        GROUP BY peer
        HAVING sum(InAndOut.in_ - InAndOut.out_) > make_time(N / 60, N - N / 60 * 60, 0.);
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part24('cursor_name', 120);
    FETCH ALL IN "cursor_name";
COMMIT;
---------------????-------------
-- SELECT * FROM timetracking
-- WHERE date = current_date - 1
---------------????-------------

-- 25) Определить для каждого месяца процент ранних входов
CREATE OR REPLACE FUNCTION countVisit(peer_ text, mintime time DEFAULT '24:00:00') 
    RETURNS integer AS $$ 
    SELECT count(*) FROM (
                            SELECT tt.peer, tt.date FROM timetracking tt
                            WHERE tt.peer = peer_
                            GROUP BY 1, 2
                            HAVING min("time") < mintime
                         ) as res
$$ LANGUAGE sql;

CREATE OR REPLACE PROCEDURE pr_part25(IN ref refcursor) AS $$
    BEGIN
    OPEN ref FOR
        WITH Months AS 
        (
            SELECT gs::date AS "Month"
            FROM generate_series('2000-01-31', '2000-12-31', interval '1 month') AS gs
        ), VisitsInMonthOfBirth AS 
        (
            SELECT 
                    TO_CHAR("Month"::date, 'Month') AS "Month", 
                    sum(countVisit(p.nickname::text)) AS "AllVisits", 
                    sum(countVisit(p.nickname::text, '12:00:00')) AS "EarlyVisits"
            FROM Months
            LEFT JOIN peers p ON EXTRACT(MONTH FROM "Month") = EXTRACT(MONTH FROM p.birthday::date)
            GROUP BY "Month"
        )

        SELECT "Month", CASE WHEN "AllVisits" = 0
                        THEN 0
                        ELSE ("EarlyVisits" * 100 / "AllVisits") 
                        END AS "EarlyEntries"
        FROM VisitsInMonthOfBirth
        ORDER BY to_date("Month", 'Mon');
    END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL pr_part25('cursor_name');
    FETCH ALL IN "cursor_name";
COMMIT;