
-- https://www.postgresql.org/docs/current/sql-cluster.html
-- https://www.postgresql.org/docs/current/rules-materializedviews.html

DROP TRIGGER IF EXISTS trigger_refresh_mv_votos_view ON users CASCADE;
DROP FUNCTION IF EXISTS refresh_mv_votos_view;
DROP MATERIALIZED VIEW IF EXISTS mv_votos CASCADE;


--==============================================  V1
-- Q1
--==============================================

create index questions_creationdate_idx on questions (creationdate);
cluster questions using questions_creationdate_idx;

create index answers_creationdate_idx on answers (creationdate);
cluster answers using answers_creationdate_idx;

create index comments_creationdate_idx on comments (creationdate);
cluster comments using comments_creationdate_idx;

--============================================== V2
-- Q2
--==============================================

create index votes_creationdate_idx on votes (creationdate);
cluster votes using votes_creationdate_idx;


-- create materialized view mv_votos as
-- SELECT postid, v.creationdate
-- FROM votes v
--          JOIN votestypes vt ON vt.id = v.votetypeid
-- WHERE vt.name = 'AcceptedByOriginator';

-- create index mv_votos_creationdate_idx on mv_votos (creationdate);
-- cluster mv_votos using mv_votos_creationdate_idx;

-- -- Refresh the view on votes create and delete triggers
-- -- refresh materialized view mv_votos;


-- CREATE OR REPLACE FUNCTION refresh_mv_votos_view()
-- RETURNS TRIGGER AS
-- $$
-- BEGIN
--     REFRESH MATERIALIZED VIEW mv_votos;
--     RETURN NEW;
-- END;
-- $$
-- LANGUAGE plpgsql;

-- CREATE TRIGGER trigger_refresh_mv_votos_view
-- AFTER INSERT OR UPDATE OR DELETE ON votes
-- FOR EACH STATEMENT
-- EXECUTE FUNCTION refresh_mv_votos_view();