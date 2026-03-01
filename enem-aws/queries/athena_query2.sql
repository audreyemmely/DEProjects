-- Evolução por raça/cor entre 2023 e 2024
SELECT
    ano,
    CASE tp_cor_raca
        WHEN 1 THEN 'Branca'
        WHEN 2 THEN 'Preta'
        WHEN 3 THEN 'Parda'
        WHEN 4 THEN 'Amarela'
        WHEN 5 THEN 'Indígena'
    END                                     AS cor_raca,
    COUNT(nu_inscricao)                     AS total_participantes,
    ROUND(AVG(nu_nota_mt), 1)               AS media_matematica,
    ROUND(AVG(nu_nota_lc), 1)               AS media_linguagens,
    ROUND(AVG(nu_nota_redacao), 1)          AS media_redacao,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN ano = '2023' AND tp_escola = 2                        THEN 1
                WHEN ano = '2024' AND tp_dependencia_adm_esc IN (1, 2, 3) THEN 1
                ELSE 0
            END
        ) / COUNT(*), 1
    )                                       AS pct_escola_publica
FROM enem_db.enem
WHERE ano IN ('2023', '2024')
  AND tp_presenca_mt = 1
  AND tp_presenca_lc = 1
  AND nu_nota_mt > 0
  AND nu_nota_redacao > 0
  AND tp_cor_raca BETWEEN 1 AND 5
GROUP BY ano, tp_cor_raca
ORDER BY tp_cor_raca, ano