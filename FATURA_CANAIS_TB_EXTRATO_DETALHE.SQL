{{
  config(
    materialized='incremental',
    on_schema_change="append_new_columns",
    unique_key = "SERNO"
  )
}}

{% set table_check_query %}
    select 1
    from `{{ this.schema }}.INFORMATION_SCHEMA.TABLES`
    where table_name = 'TB_PID_EXTRATO_TAS__EXTRATO_DETALHE' and table_catalog = '{{this.database}}'
{% endset %}

{% set table_exists = run_query(table_check_query) %}
{% set serno_inicial = -4760000000 %}
{% set serno_minimo = serno_inicial %}

{% if table_exists and table_exists.rows|length > 0 %}
    {% set get_minimum_serno %}
        select coalesce(min(SERNO), {{serno_inicial}})
        from `{{ this.database }}.{{ this.schema }}.TB_PID_EXTRATO_TAS__EXTRATO_DETALHE`
    {% endset %}
    {% set result = run_query(get_minimum_serno) %}
    {% if result and result.rows|length > 0 %}
        {% set serno_minimo = result.columns[0].values()[0] %}
    {% endif %}
{% endif %}

with
CTE_BASE as (
    select *
    from {{ ref('TB_WRK_EXTRATO_TAS__CANAIS_ROUTER') }}
    where route_group = 'RESUMIDO_DETALHE'
),

VARS as (
  select PRODUTO, EMPRESA, DATA_CORTE, NOME_ARQUIVO_RESUMIDO as NOME_ARQUIVO
  from {{ ref("TB_WRK_EXTRATO_TAS__CONTA") }}
),

CTE_JOIN_BASE_X_VARS as (
    select *
    from CTE_BASE
    cross join vars
),

CTE_SEQ_START_VALUE as (
    select {{serno_minimo}} as start_value
),

CTE_BASE_NUMBERED as (
    select
        *,
        row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as rn
    from CTE_JOIN_BASE_X_VARS
),

CTE_SEQ_GEN as (
    select
        (select start_value from CTE_SEQ_START_VALUE)
        - row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as serno_falso,
        row_number() over (order by transacao_conta, transacao_cartao, transacao_trxn_date) as rn
    from CTE_BASE
),

CTE_SEQ_GEN_NUMBERED as (
    select
        serno_falso * -1 as serno_final,
        row_number() over (order by serno_falso) as rn
    from CTE_SEQ_GEN
),

CTE_JOINED as (
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
        NOME_ARQUIVO as nm_arqori,
        PRODUTO as nm_prodfat,
        case
            when upper(EMPRESA) = 'CARF' then 1
            when upper(EMPRESA) = 'SAMS' then 6
            else 2
        end as id_emprs
    from CTE_BASE_NUMBERED as b
    inner join cte_seq_gen_numbered as s on b.rn = s.rn
)

select
    CYCLE_DATE,
    CPF,
    CONTA,
    transacao_cartao as CARTAO,
    transacao_trxn_date as TRXN_DATE,
    DESCR_1,
    transacao_descr_2 as DESCR_2,
    transacao_amt_trxn as AMT_TRXN,
    SERNO,
    CACCSERNO,
    DT_CARGA,
    NM_ARQORI,
    NM_PRODFAT,
    ID_EMPRS
from CTE_JOINED