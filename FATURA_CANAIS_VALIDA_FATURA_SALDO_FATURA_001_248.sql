{ config(materialized='table') }

with wrk as (
  select * from {{ ref('FATURA_CANAIS_VALIDA_FATURA_WRK') }}
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
  where route_group = 'SALDO_FATURA_001_248'
)

select
  out_cycle_date as cycle_date,
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
  serno_ods as ods_conta_serno
from filtered;