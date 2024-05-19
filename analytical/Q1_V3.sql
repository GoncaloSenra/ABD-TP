EXPLAIN ANALYZE
WITH recent_questions AS (
    SELECT owneruserid, count(distinct id) q_total
    FROM questions
    WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    group by owneruserid
),
     recent_answers AS (
         SELECT owneruserid, count(distinct id) a_total
         FROM answers
         WHERE creationdate BETWEEN now() - interval '6 months' AND now()
         group by owneruserid
     ),
     recent_comments AS (
         SELECT userid, count(distinct id) c_total
         FROM comments
         WHERE creationdate BETWEEN now() - interval '6 months' AND now()
         group by userid
     )
SELECT id, displayname,
    coalesce(q_total,0) + coalesce(a_total,0) + coalesce(c_total,0) total
FROM users u
 LEFT JOIN recent_questions q ON q.owneruserid = u.id
 LEFT JOIN recent_answers a ON a.owneruserid = u.id
 LEFT JOIN recent_comments c ON c.userid = u.id
ORDER BY total DESC, u.id
LIMIT 100;
