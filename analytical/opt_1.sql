
-- Q1

select tablename, indexname, indexdef from pg_indexes where schemaname = 'public';

create index questions_owneruserid_idx on questions (owneruserid);
create index answers_owneruserid_idx on answers (owneruserid);
create index comments_owneruserid_idx on comments (userid);

drop index questions_owneruserid_idx;
drop index answers_owneruserid_idx;
drop index comments_owneruserid_idx;

create index questions_creationdate_idx on questions (creationdate);
create index answers_creationdate_idx on answers (creationdate);
create index comments_creationdate_idx on comments (creationdate);

-- https://www.postgresql.org/docs/current/sql-cluster.html
cluster questions using questions_creationdate_idx;
cluster answers using answers_creationdate_idx;
cluster comments using comments_creationdate_idx;

drop index questions_creationdate_idx;
drop index answers_creationdate_idx;
drop index comments_creationdate_idx;

-- select count(*) u from users; -- 1492696
-- select count(*) q from questions; -- 3000000
-- select count(*) a from answers; -- 4427303
-- select count(*) c from comments; -- 11242804

show shared_buffers ; -- 128MB
show work_mem ; -- 4MB

show config_file ;

