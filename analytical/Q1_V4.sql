SELECT u.id, u.displayname,
--        (sum(case when q.id IS NOT NULL then 1 else 0 end) +
--         sum(case when a.id IS NOT NULL then 1 else 0 end) +
--         sum(case when c.id IS NOT NULL then 1 else 0 end)) total
       count(DISTINCT q.id) + count(DISTINCT a.id) + count(DISTINCT c.id) total
FROM users u
         LEFT JOIN questions q ON q.owneruserid = u.id
    AND q.creationdate BETWEEN now() - interval '6 months' AND now()
         LEFT JOIN answers a ON a.owneruserid = u.id
    AND a.creationdate BETWEEN now() - interval '6 months' AND now()
         LEFT JOIN comments c ON c.userid = u.id
    AND c.creationdate BETWEEN now() - interval '6 months' AND now()
GROUP BY u.id, u.displayname
ORDER BY total DESC, id
LIMIT 100;
