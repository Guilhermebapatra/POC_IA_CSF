{{
  config(materialized='table')
}}

with
cte_base as (
    select * from {{ ref('FATURA_CANAIS_WRK') }}
),

cte_filtered as (
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
    from cte_base
    where route_group = 'REJEITADO'
)

select
    conta_conta,
    serno_ods as serno,
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
    linha_digitavel,
    serno_ods as ods_conta_serno
from cte_filtered;