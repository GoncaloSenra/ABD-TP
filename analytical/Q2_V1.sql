WITH
    votos as (
        SELECT postid
        FROM votes v
        JOIN votestypes vt ON vt.id = v.votetypeid
        WHERE vt.name = 'AcceptedByOriginator'
          AND v.creationdate >= NOW() - INTERVAL '5 year'
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
    buckets AS (
        SELECT year,
            generate_series(0, (
                SELECT cast(max(reputation) as int)
                FROM utilizadores
                WHERE ano = year
            ), 5000) AS reputation_range
        FROM (
            SELECT generate_series(2008, extract(year FROM NOW())) AS year
        ) years
        GROUP BY 1, 2
    )
SELECT year, reputation_range, count(u.id) total
FROM buckets
LEFT JOIN utilizadores u ON  u.ano = year
    AND u.rep = reputation_range
GROUP BY 1, 2
ORDER BY 1, 2;
