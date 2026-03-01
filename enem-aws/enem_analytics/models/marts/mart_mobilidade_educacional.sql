-- Escolaridade da mãe × desempenho × contexto socioeconômico
SELECT
    ano,
    CASE escolaridade_mae_cod
        WHEN 'A' THEN '1. Nunca estudou'
        WHEN 'B' THEN '2. Fundamental incompleto'
        WHEN 'C' THEN '3. Fundamental completo'
        WHEN 'D' THEN '4. Médio incompleto'
        WHEN 'E' THEN '5. Médio completo'
        WHEN 'F' THEN '6. Superior completo'
        WHEN 'G' THEN '7. Pós-graduação'
    END                             AS escolaridade_mae,
    COUNT(*)                        AS total,
    ROUND(AVG(nota_matematica), 1)  AS media_matematica,
    ROUND(AVG(nota_redacao), 1)     AS media_redacao,
    ROUND(AVG(nota_media_geral), 1) AS media_geral,
    ROUND(
        100.0 * SUM(CASE WHEN escola_publica THEN 1 ELSE 0 END) / COUNT(*), 1
    )                               AS pct_escola_publica,
    ROUND(AVG(CAST(pessoas_na_residencia AS DOUBLE)), 1) AS media_pessoas_residencia
FROM {{ ref('stg_enem') }}
WHERE escolaridade_mae_cod IN ('A','B','C','D','E','F','G')
GROUP BY ano, escolaridade_mae_cod
ORDER BY ano, escolaridade_mae_cod