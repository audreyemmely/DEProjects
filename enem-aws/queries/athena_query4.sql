--Renda familiar × tipo de escola × desempenho
SELECT
    CASE q007
        WHEN 'A' THEN '01. Nenhuma renda'
        WHEN 'B' THEN '02. Até R$1.412'
        WHEN 'C' THEN '03. R$1.412 a R$2.118'
        WHEN 'D' THEN '04. R$2.118 a R$2.824'
        WHEN 'E' THEN '05. R$2.824 a R$3.530'
        WHEN 'F' THEN '06. R$3.530 a R$4.236'
        WHEN 'G' THEN '07. R$4.236 a R$5.648'
        WHEN 'H' THEN '08. R$5.648 a R$7.060'
        ELSE             '09. Acima de R$7.060'
    END                                     AS faixa_renda,
    CASE
        WHEN tp_dependencia_adm_esc IN (1,2,3) THEN 'Pública'
        WHEN tp_dependencia_adm_esc = 4        THEN 'Privada'
        ELSE                                        'Não informado'
    END                                     AS tipo_escola,
    COUNT(nu_inscricao)                     AS total,
    ROUND(AVG(nu_nota_mt), 1)               AS media_matematica,
    ROUND(AVG(nu_nota_redacao), 1)          AS media_redacao,
    ROUND(AVG(nu_nota_cn), 1)               AS media_ciencias
FROM enem_db.enem
WHERE ano = '2024'
  AND tp_presenca_mt = 1
  AND nu_nota_mt > 0
  AND nu_nota_redacao > 0
  AND q007 IS NOT NULL
  AND tp_dependencia_adm_esc IN (1, 2, 3, 4)
GROUP BY
    -- Agrupa pelas expressões, não pelas colunas brutas
    CASE q007
        WHEN 'A' THEN '01. Nenhuma renda'
        WHEN 'B' THEN '02. Até R$1.412'
        WHEN 'C' THEN '03. R$1.412 a R$2.118'
        WHEN 'D' THEN '04. R$2.118 a R$2.824'
        WHEN 'E' THEN '05. R$2.824 a R$3.530'
        WHEN 'F' THEN '06. R$3.530 a R$4.236'
        WHEN 'G' THEN '07. R$4.236 a R$5.648'
        WHEN 'H' THEN '08. R$5.648 a R$7.060'
        ELSE             '09. Acima de R$7.060'
    END,
    CASE
        WHEN tp_dependencia_adm_esc IN (1,2,3) THEN 'Pública'
        WHEN tp_dependencia_adm_esc = 4        THEN 'Privada'
        ELSE                                        'Não informado'
    END
ORDER BY faixa_renda, tipo_escola