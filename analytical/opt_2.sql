
-- Q2

select tablename, indexname, indexdef from pg_indexes where schemaname = 'public';

select * from votestypes;

create index votestypes_name_idx on votestypes(name);

create index votes_creationdate_idx on votes (creationdate);
create index votes_postid_idx on votes (postid);

cluster votes using votes_creationdate_idx;

-- https://www.postgresql.org/docs/current/rules-materializedviews.html

create materialized view mv_votos as
    SELECT postid, v.creationdate
    FROM votes v
    JOIN votestypes vt ON vt.id = v.votetypeid
    WHERE vt.name = 'AcceptedByOriginator';

create index mv_votos_creationdate_idx on mv_votos (creationdate);
cluster mv_votos using mv_votos_creationdate_idx;

-- Refresh the view on votos create and delete
-- refresh materialized view mv_votos;

select count(*) from mv_votos;

show shared_buffers ;
show work_mem;

show max_parallel_workers_per_gather; -- default 2
show parallel_setup_cost;
show parallel_tuple_cost;
