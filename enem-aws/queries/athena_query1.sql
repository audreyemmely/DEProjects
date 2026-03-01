-- Impacto socioeconômico no desempenho (cruzando questionário com notas)
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
        WHEN 'I' THEN '09. R$7.060 a R$8.472'
        WHEN 'J' THEN '10. R$8.472 a R$9.884'
        WHEN 'K' THEN '11. R$9.884 a R$11.296'
        WHEN 'L' THEN '12. R$11.296 a R$12.708'
        WHEN 'M' THEN '13. R$12.708 a R$14.120'
        WHEN 'N' THEN '14. R$14.120 a R$16.944'
        WHEN 'O' THEN '15. R$16.944 a R$21.180'
        WHEN 'P' THEN '16. R$21.180 a R$28.240'
        WHEN 'Q' THEN '17. Acima de R$28.240'
    END                             AS faixa_renda,
    CASE tp_sexo
        WHEN 'M' THEN 'Masculino'
        WHEN 'F' THEN 'Feminino'
    END                             AS sexo,
    COUNT(nu_inscricao)             AS total,
    ROUND(AVG(nu_nota_mt), 1)       AS media_matematica,
    ROUND(AVG(nu_nota_redacao), 1)  AS media_redacao,
    ROUND(AVG(nu_nota_ch), 1)       AS media_humanas
FROM enem_db.enem
WHERE ano = '2024'
  AND tp_presenca_mt = 1
  AND nu_nota_mt > 0
  AND q007 IS NOT NULL
GROUP BY q007, tp_sexo
ORDER BY q007, tp_sexo