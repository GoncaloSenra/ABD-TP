with dados as (
    SELECT owneruserid user_id, count(distinct id) total
    FROM questions
    WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    group by owneruserid
    union all
    SELECT owneruserid, count(distinct id) total
    FROM answers
    WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    group by owneruserid
    union all
    SELECT userid, count(distinct id) total
    FROM comments
    WHERE creationdate BETWEEN now() - interval '6 months' AND now()
    group by userid
)
SELECT u.id, u.displayname, sum(coalesce(total, 0)) total
FROM users u
LEFT JOIN dados d ON d.user_id = u.id
group by u.id, u.displayname
ORDER BY total DESC
LIMIT 100;
