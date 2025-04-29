{{
  config(
    materialized='incremental',
    on_schema_change="append_new_columns",
    unique_key = ["CYCLE_DATE", "ACC_SERNO"]
    )
}}

with
CTE_BASE as (
    select *
    from {{ ref('TB_WRK_EXTRATO_TAS__CANAIS_ROUTER') }}
    where route_group = "RESUMIDO_DETALHE"
),

VARS as (
  select PRODUTO, EMPRESA, DATA_CORTE, NOME_ARQUIVO_RESUMIDO as NOME_ARQUIVO
  from {{ ref("TB_WRK_EXTRATO_TAS__CONTA") }}
),

CTE_JOIN_BASE_X_VARS as (
    select *
    from CTE_BASE
    cross join VARS
),

CTE_DISTINCT_RESUMIDO as (
    select distinct
        cycle_date,
        cliente_cpf,
        conta_conta,
        serno_ods,
        billing_date_ini,
        billing_date_fim,
        conta_total_credits,
        conta_total_debits,
        conta_mindueamount,
        conta_total_paymetns,
        conta_opening_balance,
        conta_closing_balance,
        conta_instalment_amt,
        dt_carga,
        linha_digitavel,
        PRODUTO,
        EMPRESA,
        DATA_CORTE,
        NOME_ARQUIVO
    from CTE_JOIN_BASE_X_VARS
),

CTE_FINAL_TRANSFORM as (
    select
        cycle_date,
        cliente_cpf as cpf,
        conta_conta,
        0 as serno,
        serno_ods as acc_serno,
        billing_date_ini,
        billing_date_fim,
        conta_total_credits,
        conta_total_debits,
        conta_mindueamount * -1 as mindueamount,
        conta_total_paymetns,
        conta_opening_balance * -1 as opening_balance,
        conta_closing_balance * -1 as closing_balance,
        conta_instalment_amt * -1 as instalment_amt,
        dt_carga,
        linha_digitavel,
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
            when upper(EMPRESA) = 'CARF' then 1
            when upper(EMPRESA) = 'SAMS' then 6
            else 2
        end as id_emprs
    from CTE_DISTINCT_RESUMIDO
)

select
    CYCLE_DATE,
    CPF,
    conta_conta as CONTA,
    SERNO,
    ACC_SERNO,
    BILLING_DATE_INI,
    BILLING_DATE_FIM,
    conta_total_credits as TOTAL_CREDITS,
    conta_total_debits as TOTAL_DEBITS,
    conta_total_paymetns as TOTAL_PAYMETNS,
    OPENING_BALANCE,
    CLOSING_BALANCE,
    MINDUEAMOUNT,
    INSTALMENT_AMT,
    DT_CARGA,
    linha_digitavel as CD_BARRAFATURA,
    NM_ARQORI,
    NM_PRODFAT,
    ID_EMPRS
from CTE_FINAL_TRANSFORM

