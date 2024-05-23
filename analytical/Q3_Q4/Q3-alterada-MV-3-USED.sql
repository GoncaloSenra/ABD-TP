SELECT tagname, round(avg(total), 3), count(*)
FROM q3_subquery_mv3
GROUP BY tagname
HAVING count(*) > 10
ORDER BY 2 DESC, 3 DESC, tagname;

--10/20ms