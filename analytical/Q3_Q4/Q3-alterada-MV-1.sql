SELECT tagname, round(avg(total), 3), count(*)
FROM (
    SELECT t.tagname, qt.questionid, count(*) AS total
    FROM q3_subquery_mv1 t
    JOIN questionstags qt ON qt.tagid = t.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    GROUP BY t.tagname, qt.questionid
)
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;

--usando a vista materializada - média 20ms/25ms