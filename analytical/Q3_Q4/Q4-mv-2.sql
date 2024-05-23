EXPLAIN ANALYZE SELECT date_bin('1 minute', date, '2008-01-01 00:00:00'), count(*)
FROM badges_mv_2
GROUP BY 1
ORDER BY 1;


-- corre a uma média de 300ms