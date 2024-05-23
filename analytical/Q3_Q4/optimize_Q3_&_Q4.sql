-- Q4 otimização --------------------

DROP TRIGGER IF EXISTS trigger_refresh_badges_mv_1_view ON users CASCADE;
DROP FUNCTION IF EXISTS refresh_badges_mv_1_view;
DROP MATERIALIZED VIEW IF EXISTS badges_mv_1;

DROP INDEX IF EXISTS idx_badges;


CREATE MATERIALIZED VIEW badges_mv_1 AS
SELECT date, class, name
FROM badges
WHERE NOT tagbased
AND userid <> -1;

CREATE OR REPLACE FUNCTION refresh_badges_mv_1_view()
RETURNS TRIGGER AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW badges_mv_1;
    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER trigger_refresh_badges_mv_1_view
AFTER INSERT OR UPDATE OR DELETE ON badges
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_badges_mv_1_view();


-- Q3 otimização ------------------------------------


DROP TRIGGER IF EXISTS trigger_refresh_q3_subquery_mv3_view ON users CASCADE;
DROP FUNCTION IF EXISTS refresh_q3_subquery_mv3_view;
DROP MATERIALIZED VIEW IF EXISTS q3_subquery_mv3 CASCADE;

DROP INDEX IF EXISTS idx_answers_parentid;

CREATE INDEX idx_answers_parentid ON answers (parentid); 

-- CREATE MATERIALIZED VIEW q3_subquery_mv3 AS
-- SELECT t.tagname, qt.questionid, count(*) AS total
-- FROM tags t
-- JOIN questionstags qt ON qt.tagid = t.id
-- LEFT JOIN answers a ON a.parentid = qt.questionid
-- GROUP BY t.tagname, qt.questionid;


-- CREATE OR REPLACE FUNCTION refresh_q3_subquery_mv3_view()
-- RETURNS TRIGGER AS
-- $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW q3_subquery_mv3;
--     RETURN NEW;
-- END;
-- $$
-- LANGUAGE plpgsql;

-- CREATE TRIGGER trigger_refresh_q3_subquery_mv3_view
-- AFTER INSERT OR UPDATE OR DELETE ON votes
-- FOR EACH STATEMENT
-- EXECUTE FUNCTION refresh_q3_subquery_mv3_view();


