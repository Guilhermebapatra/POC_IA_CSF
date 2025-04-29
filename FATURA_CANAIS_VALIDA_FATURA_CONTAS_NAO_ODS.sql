{
  config(
    materialized='table',
    post_hook=[
      "update {{ source('bigquery', 'tb_seq_ctrl') }} set last_value = (select max(SERNO_ODS) from {{ this }}) where table_name = 'CONTAS_NAO_ODS' and column_name = 'SERNO_ODS';"
    ]
  )
}

with wrk as (
  select * from {{ ref('FATURA_CANAIS_VALIDA_FATURA_WRK') }}
),

contas_nao_cadastradas as (
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
    (select coalesce(last_value, 0) from {{ source('bigquery', 'tb_seq_ctrl') }} where table_name = 'CONTAS_NAO_ODS' and column_name = 'SERNO_ODS') +
      row_number() over (order by conta_conta) as serno_ods
  from wrk
  where route_group = 'CONTA_NAO_CADASTRO_ODS'
),

faturas_validas_carregar as (
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
),

final as (
  select * from contas_nao_cadastradas
  union all
  select * from faturas_validas_carregar
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
  serno_ods
from final;