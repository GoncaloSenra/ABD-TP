
-- https://www.postgresql.org/docs/current/sql-cluster.html
-- https://www.postgresql.org/docs/current/rules-materializedviews.html

--==============================================
-- Q1
--==============================================

create index questions_creationdate_idx on questions (creationdate);
cluster questions using questions_creationdate_idx;

create index answers_creationdate_idx on answers (creationdate);
cluster answers using answers_creationdate_idx;

create index comments_creationdate_idx on comments (creationdate);
cluster comments using comments_creationdate_idx;

--==============================================
-- Q2
--==============================================

create index votes_creationdate_idx on votes (creationdate);
cluster votes using votes_creationdate_idx;


create materialized view mv_votos as
SELECT postid, v.creationdate
FROM votes v
         JOIN votestypes vt ON vt.id = v.votetypeid
WHERE vt.name = 'AcceptedByOriginator';

create index mv_votos_creationdate_idx on mv_votos (creationdate);
cluster mv_votos using mv_votos_creationdate_idx;

-- Refresh the view on votes create and delete triggers
-- refresh materialized view mv_votos;
