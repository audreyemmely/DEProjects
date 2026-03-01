-- Desempenho médio por estado e ano
SELECT
    ano,
    uf,
    regiao,
    COUNT(*)                        AS total_participantes,
    ROUND(AVG(nota_matematica), 1)  AS media_matematica,
    ROUND(AVG(nota_ciencias), 1)    AS media_ciencias,
    ROUND(AVG(nota_humanas), 1)     AS media_humanas,
    ROUND(AVG(nota_linguagens), 1)  AS media_linguagens,
    ROUND(AVG(nota_redacao), 1)     AS media_redacao,
    ROUND(AVG(nota_media_geral), 1) AS media_geral,
    ROUND(
        100.0 * SUM(CASE WHEN escola_publica THEN 1 ELSE 0 END) / COUNT(*), 1
    )                               AS pct_escola_publica
FROM {{ ref('stg_enem') }}
GROUP BY ano, uf, regiao
ORDER BY ano, media_geral DESC