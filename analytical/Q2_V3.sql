WITH
    votos as (
        SELECT postid
        FROM mv_votos
        WHERE creationdate >= NOW() - INTERVAL '5 year'
    ),
    respostas as (
        SELECT a.owneruserid
        FROM answers a
        WHERE exists (select 1 from votos v where v.postid = a.id)
    ),
    utilizadores as (
        SELECT id, extract(year FROM u.creationdate) ano, reputation, floor(u.reputation / 5000) * 5000 rep
        FROM users u
        WHERE exists (select 1 from respostas r where r.owneruserid = u.id)
    ),
    years as (
        SELECT generate_series(2008, extract(year FROM NOW())) AS year
    ),
    max_reputations as (
        SELECT year, cast(max(reputation) as int) rep
        FROM utilizadores u
        join years y on y.year = u.ano
        WHERE ano = year
        group by year
    ),
    buckets AS (
        SELECT year, generate_series(0, rep, 5000) AS reputation_range
        FROM max_reputations
        GROUP BY 1, 2
    )
SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN utilizadores u ON  u.ano = year
    AND u.rep = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;
