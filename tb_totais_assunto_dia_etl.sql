{{
  config(
    materialized='incremental',
    on_schema_change="append_new_columns",
    unique_key=["DT_TOTALASSUNETL", "ID_ASSUNNEGOCETL", "DL_RESULTGROUPBY"]
  )
}}

with VALIDA_FATURA_ROUTER as (
  select *
  from {{ref ("TB_WRK_EXTRATO_TAS__VALIDA_FATURA_ROUTER")}}
),

TRANSACAO_ROUTER as (
    select
        ROUTE_GROUP
    from {{ ref('TB_WRK_EXTRATO_TAS__TRANSACAO_ROUTER') }}
    where ROUTE_GROUP = 'TRANSACAO_REJEITADA_VALOR_ZERADO'
),

CANAIS_ROUTER as (
    select *
    from {{ ref('TB_WRK_EXTRATO_TAS__CANAIS_ROUTER') }}
    where route_group = 'REJEITADO'
),

CTE_EXTRATO_RESUMIDO as (
    select
      CYCLE_DATE,
      CPF,
      CONTA,
      SERNO,
      ACC_SERNO,
      BILLING_DATE_INI,
      BILLING_DATE_FIM,
      TOTAL_CREDITS,
      TOTAL_DEBITS,
      MINDUEAMOUNT,
      TOTAL_PAYMETNS,
      OPENING_BALANCE,
      CLOSING_BALANCE,
      INSTALMENT_AMT,
      NM_ARQORI,
      NM_PRODFAT,
      ID_EMPRS
    from
      {{ ref('TB_PID_EXTRATO_TAS__EXTRATO_RESUMIDO') }}
),

VARS as (
  select distinct PRODUTO, EMPRESA, DATA_CORTE, NOME_ARQUIVO_RESUMIDO as NOME_ARQUIVO
  from {{ ref("TB_WRK_EXTRATO_TAS__CONTA") }}
),

CTE_JOIN_EXTRATO_RESUMIDO_VARS as (
    select *
    from cte_extrato_resumido
    cross join VARS
),

CTE_FILTERED_EXTRATO_RESUMIDO as (
    select *
    from CTE_JOIN_EXTRATO_RESUMIDO_VARS
    where NM_ARQORI = NOME_ARQUIVO
),

AGG_TOTAL_FATURAS_ARQUIVO_ORIGEM as (
    select
        parse_date('%d%m%Y', substr(DATA_CORTE, 7, 2)||substr(DATA_CORTE, 5, 2)||substr(DATA_CORTE, 1, 4)) as dt_totalassunetl,
        73 as id_assunnegocetl,
        'Fatura APP, Portal e TAS - ' || ' ' || PRODUTO || ' ' || replace(EMPRESA, '_', '') || ' ' || substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4) as dl_resultgroupby,
        count(1) as qt_totalassunorietl,
        0 as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
          when EMPRESA = 'CARF' then 'CARREFOUR'
          when EMPRESA = 'SAM_' then 'SAMS'
          else 'ATACADAO'
        end as nm_emprs,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_inicarga,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_fimcarga
  from VALIDA_FATURA_ROUTER
  cross join VARS
  where route_group = 'TOTAL_FATURAS_VALIDAS_CARREGAR'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

CTE_JOIN as (
    select
        cte_extrato_resumido.CONTA,
        cte_extrato_resumido.NM_ARQORI,
        agg_total_faturas_arquivo_origem.NM_ARQORI as NM_ARQORI_CONTROLE,
        cte_extrato_resumido.PRODUTO,
        cte_extrato_resumido.EMPRESA,
        cte_extrato_resumido.DATA_CORTE,
        cte_extrato_resumido.NOME_ARQUIVO
    from
        cte_filtered_extrato_resumido as cte_extrato_resumido
    right outer join
        agg_total_faturas_arquivo_origem as agg_total_faturas_arquivo_origem
    on
        agg_total_faturas_arquivo_origem.NM_ARQORI = cte_extrato_resumido.NM_ARQORI
),

CTE_AGG as (
    select
        count(CONTA) as QUANTIDADE_FATURAS_PROCESSADA
    from
        cte_join
),


CTE_JOIN_AGG_VARS as (
    select *
    from CTE_AGG
    cross join VARS
),

CTE_UPDATE_FATURAS_ARQUIVO_ORIGEM as (
    select
        parse_date('%d%m%Y', substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4)) as dt_totalassunetl,
        73 as id_assunnegocetl,
        'Fatura APP, Portal e TAS - ' || ' ' || PRODUTO || ' ' || replace(EMPRESA, '_', '') || ' ' || substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4) as dl_resultgroupby,
        QUANTIDADE_FATURAS_PROCESSADA as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
            when upper(EMPRESA) = 'CARF' then 'CARREFOUR'
            else 'ATACADAO'
        end as nm_emprs,
        cast(timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as timestamp) as dt_fimcarga
    from
        cte_join_agg_VARS
),

CTE_UPDATE_X_FATURAS_ARQUIVO_ORIGEM as (
  select
    upd.dt_totalassunetl,
    upd.id_assunnegocetl,
    upd.dl_resultgroupby,
    agg.qt_totalassunorietl,
    upd.qt_totalassundestetl,
    upd.nm_arqori,
    upd.nm_prodfat,
    upd.nm_emprs,
    agg.dt_inicarga,
    upd.dt_fimcarga
  from AGG_TOTAL_FATURAS_ARQUIVO_ORIGEM as agg
  join cte_update_faturas_arquivo_origem as upd
  on agg.id_assunnegocetl = upd.id_assunnegocetl
     and agg.nm_prodfat = upd.nm_prodfat
),

AGG_CONTA_NAO_CADASTRADA_ODS as (
    select
        parse_date('%d%m%Y', substr(DATA_CORTE, 7, 2)||substr(DATA_CORTE, 5, 2)||substr(DATA_CORTE, 1, 4)) as dt_totalassunetl,
        33 as id_assunnegocetl,
        'Conta Não Encontrada no ODS' || ' ' || PRODUTO || ' ' || replace(EMPRESA, '_', '') || ' ' || substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4) as dl_resultgroupby,
        count(1) as qt_totalassunorietl,
        0 as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
          when EMPRESA = 'CARF' then 'CARREFOUR'
          when EMPRESA = 'SAM_' then 'SAMS'
          else 'ATACADAO'
        end as nm_emprs,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_inicarga,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_fimcarga
  from VALIDA_FATURA_ROUTER
  cross join VARS
  where route_group = 'CONTA_NAO_CADASTRO_ODS'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

AGG_SALDO_FATURA_001_248 as (
    select
        parse_date('%d%m%Y', substr(DATA_CORTE, 7, 2)||substr(DATA_CORTE, 5, 2)||substr(DATA_CORTE, 1, 4)) as dt_totalassunetl,
        35 as id_assunnegocetl,
        'Saldo Fatura Entre 0.01 e 2.48' || ' ' || PRODUTO || ' ' || replace(EMPRESA, '_', '') || ' ' || substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4) as dl_resultgroupby,
        count(1) as qt_totalassunorietl,
        0 as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
          when EMPRESA = 'CARF' then 'CARREFOUR'
          when EMPRESA = 'SAM_' then 'SAMS'
          else 'ATACADAO'
        end as nm_emprs,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_inicarga,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_fimcarga
  from VALIDA_FATURA_ROUTER
  cross join VARS
  where route_group = 'SALDO_FATURA_001_248'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

AGG_TRANSACAO_ZERADA as (
    select
        parse_date('%d%m%Y', substr(DATA_CORTE, 7, 2)||substr(DATA_CORTE, 5, 2)||substr(DATA_CORTE, 1, 4)) as dt_totalassunetl,
        74 as id_assunnegocetl,
        'Transação Valor Zerada ' || PRODUTO || ' ' || replace(EMPRESA, '_', '') || ' ' || substr(DATA_CORTE, 7, 2) || substr(DATA_CORTE, 5, 2) || substr(DATA_CORTE, 1, 4) as dl_resultgroupby,
        count(1) as qt_totalassunorietl,
        0 as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
            when EMPRESA = 'CARF' then 'CARREFOUR'
            when EMPRESA = 'SAM_' then 'SAMS'
            else 'ATACADAO'
        end as nm_emprs,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_inicarga,
        timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_fimcarga
    from TRANSACAO_ROUTER
    cross join VARS
    group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

AGG_FATURA_SEM_TRANSACAO as (
    select
        PARSE_DATE('%d%m%Y', substr(DATA_CORTE,7,2)||substr(DATA_CORTE,5,2)||substr(DATA_CORTE,1,4)) as dt_totalassunetl,
        34 as id_assunnegocetl,
        'Fatura Sem Transação'||' '||PRODUTO||' '||replace(EMPRESA,'_','')||' '|| substr(DATA_CORTE,7,2) || substr(DATA_CORTE,5,2) || substr(DATA_CORTE,1,4) as dl_resultgroupby,
        count(*) as qt_totalassunorietl,
        0 as qt_totalassundestetl,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
            when upper(EMPRESA) = 'CARF' then 'CARREFOUR'
            when upper(EMPRESA) = 'SAMS' then 'SAMS'
            else 'ATACADAO'
        end as nm_emprs,
        cast(timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as timestamp) as dt_inicarga,
        cast(timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as timestamp) as dt_fimcarga
    from CANAIS_ROUTER
    cross join VARS
    group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

UNIONED_DATA as (
  select * from CTE_UPDATE_X_FATURAS_ARQUIVO_ORIGEM
  union all
  select * from AGG_CONTA_NAO_CADASTRADA_ODS
  union all
  select * from AGG_SALDO_FATURA_001_248
  union all
  select * from AGG_TRANSACAO_ZERADA
  union all
  select * from AGG_FATURA_SEM_TRANSACAO
)

select
  DT_TOTALASSUNETL,
  ID_ASSUNNEGOCETL,
  DL_RESULTGROUPBY,
  QT_TOTALASSUNORIETL,
  QT_TOTALASSUNDESTETL,
  NM_ARQORI,
  NM_PRODFAT,
  NM_EMPRS,
  DT_INICARGA,
  DT_FIMCARGA
from UNIONED_DATA