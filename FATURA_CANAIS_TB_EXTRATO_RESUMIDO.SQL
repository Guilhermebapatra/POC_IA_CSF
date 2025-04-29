{{
  config(materialized='table')
}}

with
cte_base as (
    select * from {{ ref('FATURA_CANAIS_WRK') }}
    where route_group != 'REJEITADO'
      and conta_conta is not null
),

cte_distinct_resumido as (
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
        linha_digitavel
    from cte_base
),

cte_final_transform as (
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
        '{{ var("NOME_ARQUIVO") }}' as nm_arqori,
        '{{ var("PRODUTO") }}' as nm_prodfat,
        case
            when upper('{{ var("EMPRESA") }}') = 'CARF' then 1
            when upper('{{ var("EMPRESA") }}') = 'SAMS' then 6
            else 2
        end as id_emprs
    from cte_distinct_resumido
)

select
    cycle_date,
    cpf,
    conta_conta as conta,
    serno,
    acc_serno,
    billing_date_ini,
    billing_date_fim,
    conta_total_credits as total_credits,
    conta_total_debits as total_debits,
    conta_total_paymetns as total_paymetns,
    opening_balance,
    closing_balance,
    mindueamount,
    instalment_amt,
    dt_carga,
    linha_digitavel as cd_barrafatura,
    nm_arqori,
    nm_prodfat,
    id_emprs
from cte_final_transform;