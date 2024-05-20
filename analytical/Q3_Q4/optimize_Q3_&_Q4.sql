-- Q4 otimização --------------------

DROP INDEX IF EXISTS idx_badges;

--CREATE INDEX idx_badges ON badges (tagbased, class); -- usa este mas aumenta tempo
--CREATE INDEX idx_badges_class ON badges (class); -- usa este mas aumenta o tempo
-- CREATE INDEX idx_badges_date ON badges (date); //não usa este
-- CREATE INDEX idx_badges_date ON badges (name); //não usa este
-- CREATE INDEX idx_badges_date ON badges (userid); //não usa este

DROP MATERIALIZED VIEW IF EXISTS badges_mv_1;

CREATE MATERIALIZED VIEW badges_mv_1 AS
SELECT date, class, name
FROM badges
WHERE NOT tagbased
AND userid <> -1;

--CREATE MATERIALIZED VIEW badges_mv_2 AS
--SELECT *
--FROM badges
--WHERE NOT tagbased
--AND userid <> -1
--AND name NOT IN (
--        'Analytical',
--        'Census',
--        'Documentation Beta',
--        'Documentation Pioneer',
--        'Documentation User',
--        'Reversal',
--        'Tumbleweed'
--    )
--    AND class in (1, 2, 3);





-- Q3 otimização ------------------------------------


DROP INDEX IF EXISTS idx_answers_parentid;

CREATE INDEX idx_answers_parentid ON answers (parentid); --usa
--CREATE INDEX idx_tags_tagname ON tags (tagname); - não usa


--CREATE MATERIALIZED VIEW q3_subquery_mv1 AS
--SELECT t.id, t.tagname
--FROM tags t
--JOIN questionstags qt ON qt.tagid = t.id
--GROUP BY t.id
--HAVING count(*) > 10


--CREATE MATERIALIZED VIEW q3_subquery_mv2 AS
--SELECT t.id, t.tagname
--FROM tags t
--JOIN questionstags qt ON qt.tagid = t.id
--GROUP BY t.id, t.tagname;


CREATE MATERIALIZED VIEW q3_subquery_mv3 AS
SELECT t.tagname, qt.questionid, count(*) AS total
FROM tags t
JOIN questionstags qt ON qt.tagid = t.id
LEFT JOIN answers a ON a.parentid = qt.questionid
GROUP BY t.tagname, qt.questionid;




