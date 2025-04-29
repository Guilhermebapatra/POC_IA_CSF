{{ config(
    materialized='table',
    post_hook=[
      "update {{ source('bigquery', 'TB_TOTAIS_ASSUNTO_DIA_ETL') }} TGT set TGT.NM_PRODFAT = SRC.PRODUTO, TGT.NM_EMPRS = SRC.EMPRESA, TGT.NM_ARQORI = SRC.NOME_ARQUIVO, TGT.QT_TOTALASSUNDESTETL = SRC.QUANTIDADE_FATURAS_PROCESSADA_in, TGT.DT_TOTALASSUNETL = SRC.DATA_CORTE, TGT.ID_ASSUNNEGOCETL = SRC.ID_ASSUNNEGOCETL, TGT.DL_RESULTGROUPBY = SRC.DL_RESULTGROUPBY, TGT.DT_FIMCARGA = SRC.DH_FIM_CARGA from {{ this }} SRC where TGT.DT_TOTALASSUNETL = SRC.DATA_CORTE and TGT.ID_ASSUNNEGOCETL = SRC.ID_ASSUNNEGOCETL and TGT.DL_RESULTGROUPBY = SRC.DL_RESULTGROUPBY;"
    ]
) }}

with cte_totais_assunto_dia_etl1 as (
  select
    NM_ARQORI
  from {{ source('bigquery', 'TB_TOTAIS_ASSUNTO_DIA_ETL1') }}
  where NM_ARQORI = '{{ var("NOME_ARQUIVO") }}'
    and ID_ASSUNNEGOCETL = 73
),

cte_extrato_resumido as (
  select
    CONTA,
    NM_ARQORI
  from {{ source('bigquery', 'TB_EXTRATO_RESUMIDO') }}
  where NM_ARQORI = '{{ var("NOME_ARQUIVO") }}'
),

cte_join as (
  select
    t2.CONTA,
    t1.NM_ARQORI as NM_ARQORI_CONTROLE
  from cte_totais_assunto_dia_etl1 as t1
  left join cte_extrato_resumido as t2
    on t1.NM_ARQORI = t2.NM_ARQORI
),

cte_agg as (
  select
    count(CONTA) as QUANTIDADE_FATURAS_PROCESSADA,
    max(case when NM_ARQORI_CONTROLE is null then '0' else CONTA end) as NM_ARQORI_out
  from cte_join
),

cte_transform as (
  select
    case
      when NM_ARQORI_out = '0' then null
      else parse_date('%d%m%Y', substr(NM_ARQORI_out, 7, 2) || substr(NM_ARQORI_out, 5, 2) || substr(NM_ARQORI_out, 1, 4))
    end as DATA_CORTE,
    73 as ID_ASSUNNEGOCETL,
    case
      when NM_ARQORI_out = '0' then null
      else 'Fatura APP, Portal e TAS - '
           || ' '
           || substr(NM_ARQORI_out, 13, 2)
           || ' '
           || replace(substr(NM_ARQORI_out, 16, 4), '_', '')
           || ' '
           || substr(NM_ARQORI_out, 7, 2)
           || substr(NM_ARQORI_out, 5, 2)
           || substr(NM_ARQORI_out, 1, 4)
    end as DL_RESULTGROUPBY,
    coalesce(QUANTIDADE_FATURAS_PROCESSADA, 0) as QUANTIDADE_FATURAS_PROCESSADA_in,
    NM_ARQORI_out as NOME_ARQUIVO,
    substr(NM_ARQORI_out, 13, 2) as PRODUTO,
    case
      when substr(NM_ARQORI_out, 16, 4) = 'CARF' then 'CARREFOUR'
      else 'ATACADAO'
    end as EMPRESA,
    current_timestamp() as DH_FIM_CARGA
  from cte_agg
)

select
  DATA_CORTE,
  ID_ASSUNNEGOCETL,
  DL_RESULTGROUPBY,
  QUANTIDADE_FATURAS_PROCESSADA_in,
  NOME_ARQUIVO,
  PRODUTO,
  EMPRESA,
  DH_FIM_CARGA
from cte_transform;