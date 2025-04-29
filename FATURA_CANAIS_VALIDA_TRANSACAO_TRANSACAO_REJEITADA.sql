{{ config(materialized='table') }}

with base_data as (
    select
        TRANSACAO_CONTA,
        TRANSACAO_CARTAO,
        TRANSACAOI_TRXN_DATE,
        DESCR_1,
        TRANSACAO_DESCR_2,
        TRANSACAO_AMT_TRXN,
        ROUTE_GROUP
    from {{ ref('FATURA_CANAIS_VALIDA_TRANSACAO_WRK') }}
),

filtered_data as (
    select
        TRANSACAO_CONTA,
        TRANSACAO_CARTAO,
        TRANSACAOI_TRXN_DATE,
        DESCR_1,
        TRANSACAO_DESCR_2,
        TRANSACAO_AMT_TRXN
    from base_data
    where ROUTE_GROUP = 'TRANSACAO_REJEITADA_VALOR_ZERADO'
       or ROUTE_GROUP = 'TRANSACAO_IMPRESSA'
)

select
    TRANSACAO_CONTA,
    TRANSACAO_CARTAO,
    TRANSACAOI_TRXN_DATE,
    DESCR_1,
    TRANSACAO_DESCR_2,
    TRANSACAO_AMT_TRXN
from filtered_data