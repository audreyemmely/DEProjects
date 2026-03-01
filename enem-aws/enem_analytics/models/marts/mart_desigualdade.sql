-- Desigualdade: raça × tipo de escola × ano
SELECT
    ano,
    cor_raca,
    tipo_escola,
    COUNT(*)                        AS total,
    ROUND(AVG(nota_matematica), 1)  AS media_matematica,
    ROUND(AVG(nota_redacao), 1)     AS media_redacao,
    ROUND(AVG(nota_media_geral), 1) AS media_geral
FROM {{ ref('stg_enem') }}
WHERE cor_raca != 'Não declarado'
  AND tipo_escola != 'Não informado'
GROUP BY ano, cor_raca, tipo_escola
ORDER BY ano, cor_raca, tipo_escola