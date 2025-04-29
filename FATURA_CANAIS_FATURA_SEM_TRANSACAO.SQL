{{
  config(
    materialized='incremental',
    on_schema_change="append_new_columns"
    )
}}

with
CTE_BASE as (
    select *
    from {{ ref('TB_WRK_EXTRATO_TAS__CANAIS_ROUTER') }}
),

CTE_FILTERED as (
    select
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
        cliente_cpf,
        linha_digitavel
    from CTE_BASE
    where route_group = 'REJEITADO'
)

select
    CONTA_CONTA,
    serno_ods as SERNO,
    BILLING_DATE_INI,
    BILLING_DATE_FIM,
    CONTA_TOTAL_CREDITS,
    CONTA_TOTAL_DEBITS,
    CONTA_MINDUEAMOUNT,
    CONTA_TOTAL_PAYMETNS,
    CONTA_OPENING_BALANCE,
    CONTA_CLOSING_BALANCE,
    CONTA_INSTALMENT_AMT,
    DT_CARGA,
    CLIENTE_CPF,
    LINHA_DIGITAVEL,
    serno_ods as ODS_CONTA_SERNO
from CTE_FILTERED