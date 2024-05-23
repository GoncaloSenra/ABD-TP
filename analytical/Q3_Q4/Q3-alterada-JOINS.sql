SELECT tagname, round(avg(total), 3), count(*)
FROM (
    SELECT t.tagname, qt.questionid, count(*) AS total
    FROM tags t
    JOIN questionstags qt ON qt.tagid = t.id
    LEFT JOIN answers a ON a.parentid = qt.questionid
    WHERE t.id IN (
        SELECT t.id
        FROM tags t
        JOIN questionstags qt ON qt.tagid = t.id
        GROUP BY t.id
        HAVING count(*) > 10
    )
    GROUP BY t.tagname, qt.questionid
)
GROUP BY tagname
ORDER BY 2 DESC, 3 DESC, tagname;

--média 50ms/60ms