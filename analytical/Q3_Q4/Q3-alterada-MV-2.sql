EXPLAIN ANALYZE SELECT tagname, round(avg(total), 3), count(*)
FROM (
    SELECT t.tagname, qt.questionid, count(*) AS total
    FROM q3_subquery t
    JOIN questionstags qt ON qt.tagid = t.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY t.tagname, qt.questionid
)AS subquery
GROUP BY tagname
HAVING count(*) > 10
ORDER BY 2 DESC, 3 DESC, tagname;

--45/65ms