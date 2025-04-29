{{
  config(
    materialized='incremental',
    on_schema_change="append_new_columns"
    )
}}


with BASE_DATA as (
    select
        TRANSACAO_CONTA,
        TRANSACAO_CARTAO,
        TRANSACAOI_TRXN_DATE,
        DESCR_1,
        TRANSACAO_DESCR_2,
        TRANSACAO_AMT_TRXN,
        ROUTE_GROUP
    from {{ ref('TB_WRK_EXTRATO_TAS__TRANSACAO_ROUTER') }}
),

FILTERED_DATA as (
    select
        TRANSACAO_CONTA,
        TRANSACAO_CARTAO,
        TRANSACAOI_TRXN_DATE,
        DESCR_1,
        TRANSACAO_DESCR_2,
        TRANSACAO_AMT_TRXN
    from BASE_DATA
    where ROUTE_GROUP = 'TRANSACAO_REJEITADA_VALOR_ZERADO'
)

select
    TRANSACAO_CONTA,
    TRANSACAO_CARTAO,
    TRANSACAOI_TRXN_DATE,
    DESCR_1,
    TRANSACAO_DESCR_2,
    TRANSACAO_AMT_TRXN
from FILTERED_DATA