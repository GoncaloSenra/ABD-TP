SELECT u.id, u.displayname,
       coalesce(q_total,0) + coalesce(a_total,0) + coalesce(c_total,0) total
FROM users u
LEFT JOIN (
     SELECT owneruserid, count(distinct id) q_total
     FROM questions
     WHERE creationdate BETWEEN now() - interval '6 months' AND now()
     group by owneruserid
) q ON q.owneruserid = u.id
LEFT JOIN (
     SELECT owneruserid, count(distinct id) a_total
     FROM answers
     WHERE creationdate BETWEEN now() - interval '6 months' AND now()
     group by owneruserid
) a ON a.owneruserid = u.id
LEFT JOIN (
     SELECT userid, count(distinct id) c_total
     FROM comments
     WHERE creationdate BETWEEN now() - interval '6 months' AND now()
     group by userid
) c ON c.userid = u.id
ORDER BY total DESC
LIMIT 100;
