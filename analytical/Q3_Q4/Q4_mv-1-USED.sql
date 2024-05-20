EXPLAIN ANALYZE SELECT date_bin('1 minute', date, '2008-01-01 00:00:00'), count(*)
FROM badges_mv_1
WHERE class in (1, 2, 3)
    AND name NOT IN (
        'Analytical',
        'Census',
        'Documentation Beta',
        'Documentation Pioneer',
        'Documentation User',
        'Reversal',
        'Tumbleweed'
    )
GROUP BY 1;
--ORDER BY 1; --não faz diferença resultado nem tempo


-- Em conjunto com o enable off, esta dá uma média de 400/500ms