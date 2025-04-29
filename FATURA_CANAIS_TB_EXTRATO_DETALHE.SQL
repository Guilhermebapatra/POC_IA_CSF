{{
  config(
    materialized='table',
    post_hook=[
      "update {{ source('bigquery', 'tb_seq_ctrl') }} set last_value = (select coalesce(max(serno), -1) from {{ this }}) where table_name = 'TB_EXTRATO_DETALHE' and collumn_name = 'SERNO';"
    ]
  )
}}

with
cte_base as (
    select * from {{ ref('FATURA_CANAIS_WRK') }}
    where route_group != 'REJEITADO'
),

cte_seq_start_value as (
    select coalesce(max(last_value), 0) as start_value
    from {{ source('bigquery', 'tb_seq_ctrl') }}
    where table_name = 'TB_EXTRATO_DETALHE' and collumn_name = 'SERNO'
),

cte_base_numbered as (
    select
        *,
        row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as rn
    from cte_base
),

cte_seq_gen as (
    select
        (select start_value from cte_seq_start_value)
        + row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as serno_falso,
        row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as rn
    from cte_base
),

cte_seq_gen_numbered as (
    select
        serno_falso * -1 as serno_final,
        row_number() over (order by serno_falso) as rn
    from cte_seq_gen
),

cte_joined as (
    select
        b.cycle_date,
        b.cliente_cpf as cpf,
        b.transacao_conta as conta,
        b.transacao_cartao,
        b.transacao_trxn_date,
        b.descr_1,
        b.transacao_descr_2,
        b.transacao_amt_trxn,
        s.serno_final as serno,
        b.serno_ods as caccserno,
        b.dt_carga,
        '{{ var("NOME_ARQUIVO") }}' as nm_arqori,
        '{{ var("PRODUTO") }}' as nm_prodfat,
        case
            when upper('{{ var("EMPRESA") }}') = 'CARF' then 1
            when upper('{{ var("EMPRESA") }}') = 'SAMS' then 6
            else 2
        end as id_emprs
    from cte_base_numbered as b
    inner join cte_seq_gen_numbered as s on b.rn = s.rn
)

select
    cycle_date,
    cpf,
    conta,
    transacao_cartao as cartao,
    transacao_trxn_date as trxn_date,
    descr_1,
    transacao_descr_2 as descr_2,
    transacao_amt_trxn as amt_trxn,
    serno,
    caccserno,
    dt_carga,
    nm_arqori,
    nm_prodfat,
    id_emprs
from cte_joined;