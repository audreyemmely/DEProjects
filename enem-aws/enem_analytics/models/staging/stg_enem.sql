/*
  Staging do ENEM
  ---------------
  - Normaliza colunas que mudaram entre 2023 e 2024
  - Filtra apenas participantes que fizeram a prova
  - Traduz códigos numéricos em descrições legíveis
  - Cria coluna unificada de escola_publica para os dois anos
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'enem') }}
    WHERE tp_presenca_mt = 1
      AND tp_presenca_cn = 1
      AND tp_presenca_ch = 1
      AND tp_presenca_lc = 1
      AND nu_nota_mt     > 0
      AND nu_nota_redacao > 0
),

-- CTE intermediário só para converter ano para inteiro
-- assim os CTEs seguintes já usam ano como INTEGER
ano_convertido AS (
    SELECT
        *,
        CAST(ano AS INTEGER) AS ano_int
    FROM source
),

transformado AS (
    SELECT
        nu_inscricao,
        nu_sequencial,
        ano_int                                 AS ano,  -- já é INTEGER

        CASE tp_sexo
            WHEN 'M' THEN 'Masculino'
            WHEN 'F' THEN 'Feminino'
        END                                     AS sexo,

        CASE tp_cor_raca
            WHEN 1 THEN 'Branca'
            WHEN 2 THEN 'Preta'
            WHEN 3 THEN 'Parda'
            WHEN 4 THEN 'Amarela'
            WHEN 5 THEN 'Indígena'
            ELSE        'Não declarado'
        END                                     AS cor_raca,

        CASE tp_faixa_etaria
            WHEN 1  THEN 'Menor de 17'
            WHEN 2  THEN '17 anos'
            WHEN 3  THEN '18 anos'
            WHEN 4  THEN '19 anos'
            WHEN 5  THEN '20 anos'
            WHEN 6  THEN '21 anos'
            WHEN 7  THEN '22 anos'
            WHEN 8  THEN '23 anos'
            WHEN 9  THEN '24 anos'
            WHEN 10 THEN '25 anos'
            WHEN 11 THEN 'Entre 26 e 30'
            WHEN 12 THEN 'Entre 31 e 35'
            ELSE        'Acima de 35'
        END                                     AS faixa_etaria,

        CASE tp_st_conclusao
            WHEN 1 THEN 'Já concluiu'
            WHEN 2 THEN 'Concluirá este ano'
            WHEN 3 THEN 'Concluirá após este ano'
            WHEN 4 THEN 'Não concluiu'
        END                                     AS situacao_ensino_medio,

        CASE in_treineiro
            WHEN 1 THEN true
            WHEN 0 THEN false
        END                                     AS eh_treineiro,

        -- Agora ano_int é INTEGER, comparações sem aspas
        CASE
            WHEN ano_int = 2023 AND tp_escola = 2                        THEN true
            WHEN ano_int = 2024 AND tp_dependencia_adm_esc IN (1, 2, 3) THEN true
            ELSE false
        END                                     AS escola_publica,

        CASE
            WHEN ano_int = 2023 THEN
                CASE tp_escola
                    WHEN 2 THEN 'Pública'
                    WHEN 3 THEN 'Privada'
                    ELSE       'Não informado'
                END
            WHEN ano_int = 2024 THEN
                CASE
                    WHEN tp_dependencia_adm_esc IN (1,2,3) THEN 'Pública'
                    WHEN tp_dependencia_adm_esc = 4        THEN 'Privada'
                    ELSE                                        'Não informado'
                END
        END                                     AS tipo_escola,

        CASE
            WHEN ano_int = 2023 THEN q006
            WHEN ano_int = 2024 THEN q007
        END                                     AS renda_familiar_cod,

        q001                                    AS escolaridade_pai_cod,
        q002                                    AS escolaridade_mae_cod,
        CAST(q005 AS INTEGER)                   AS pessoas_na_residencia,

        sg_uf_prova                             AS uf,
        no_municipio_prova                      AS municipio,
        CASE
            WHEN sg_uf_prova IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'Norte'
            WHEN sg_uf_prova IN ('MA','PI','CE','RN','PB','PE','AL','SE','BA') THEN 'Nordeste'
            WHEN sg_uf_prova IN ('MT','MS','GO','DF') THEN 'Centro-Oeste'
            WHEN sg_uf_prova IN ('SP','RJ','ES','MG') THEN 'Sudeste'
            WHEN sg_uf_prova IN ('PR','SC','RS') THEN 'Sul'
        END                                     AS regiao,

        ROUND(nu_nota_cn, 1)                    AS nota_ciencias,
        ROUND(nu_nota_ch, 1)                    AS nota_humanas,
        ROUND(nu_nota_lc, 1)                    AS nota_linguagens,
        ROUND(nu_nota_mt, 1)                    AS nota_matematica,
        ROUND(nu_nota_redacao, 1)               AS nota_redacao,
        ROUND(
            (nu_nota_cn + nu_nota_ch + nu_nota_lc + nu_nota_mt + nu_nota_redacao) / 5, 1
        )                                       AS nota_media_geral

    FROM ano_convertido
)

SELECT * FROM transformado