{{ config(materialized='table') }}

with wrk as (
  select * from {{ ref('TB_WRK_EXTRATO_TAS__VALIDA_FATURA_ROUTER') }}
),

filtered as (
  select
    out_cycle_date,
    conta_conta,
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
    serno_ods
  from wrk
  where route_group = 'TOTAL_FATURAS_VALIDAS_CARREGAR'
)

select
  out_cycle_date as CYCLE_DATE,
  CONTA_CONTA,
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
  serno_ods as SERNO_ODS
from filtered