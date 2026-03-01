-- Mobilidade educacional: escolaridade dos pais vs desempenho dos filhos
-- Q001 = escolaridade do pai, Q002 = escolaridade da mãe
SELECT
    CASE q002  -- usando escolaridade da mãe, que tem maior correlação com desempenho
        WHEN 'A' THEN '1. Nunca estudou'
        WHEN 'B' THEN '2. Fundamental incompleto'
        WHEN 'C' THEN '3. Fundamental completo'
        WHEN 'D' THEN '4. Médio incompleto'
        WHEN 'E' THEN '5. Médio completo'
        WHEN 'F' THEN '6. Superior completo'
        WHEN 'G' THEN '7. Pós-graduação'
    END                                     AS escolaridade_mae,
    COUNT(nu_inscricao)                     AS total,
    ROUND(AVG(nu_nota_mt), 1)               AS media_matematica,
    ROUND(AVG(nu_nota_redacao), 1)          AS media_redacao,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN tp_dependencia_adm_esc IN (1, 2, 3) THEN 1
                ELSE 0
            END
        ) / COUNT(*), 1
    )                                       AS pct_escola_publica,
    ROUND(AVG(CAST(q005 AS DOUBLE)), 1)     AS media_pessoas_residencia
FROM enem_db.enem
WHERE ano = '2024'
  AND tp_presenca_mt = 1
  AND nu_nota_mt > 0
  AND nu_nota_redacao > 0
  AND q002 IN ('A','B','C','D','E','F','G')
GROUP BY q002
ORDER BY q002