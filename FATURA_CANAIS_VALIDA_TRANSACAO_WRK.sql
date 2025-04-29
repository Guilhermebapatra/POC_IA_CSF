{{ config(materialized='table') }}

with source_ff_transacao_tsys as (
    select
        NM_CONTA,
        NM_CARTAO,
        DATA_TRANSACAO,
        DESC_TRANSACAO as DESCR_2,
        SINAL_VLR_TRANSACAO,
        VLR__TRANSACAO_ORIGINAL as AMT_TRXN
    from {{ ref('TB_WRK_EXTRATO_TAS__TRANSACOES') }}
),

exp_trata_informacoes1 as (
    select
        NM_CONTA as NM_CONTA,
        NM_CARTAO,
        format_date("%Y%m%d", DATA_TRANSACAO) as DATA_TRANSACAO,
        DESCR_2,
        AMT_TRXN,
        SINAL_VLR_TRANSACAO
    from source_ff_transacao_tsys
),


exp_trata_informacoes2 as (
    select
        trim(NM_CONTA) as TRANSACAO_CONTA,
        trim(NM_CARTAO) as TRANSACAO_CARTAO,
        substr(DATA_TRANSACAO, 5, 4) || substr(DATA_TRANSACAO, 3, 2) || substr(DATA_TRANSACAO, 1, 2) as TRANSACAOI_TRXN_DATE,
        cast(null as string) as DESCR_1,
        DESCR_2 as TRANSACAO_DESCR_2,
        (cast(cast(AMT_TRXN as bigdecimal) as decimal)) * case when SINAL_VLR_TRANSACAO = 'D' then 1 else -1 end as TRANSACAO_AMT_TRXN
    from exp_trata_informacoes1
),

rtr_separa_transacao as (
    select
        TRANSACAO_CONTA,
        TRANSACAO_CARTAO,
        TRANSACAOI_TRXN_DATE,
        DESCR_1,
        TRANSACAO_DESCR_2,
        TRANSACAO_AMT_TRXN,
        case
            when trim(substr(TRANSACAO_DESCR_2, 1, 26)) = 'Cartão foi substituído por' and TRANSACAO_AMT_TRXN = 0 then 'TRANSACAO_REJEITADA_VALOR_ZERADO'
            when trim(substr(TRANSACAO_DESCR_2, 1, 24)) = 'Isenção Parcela Anuidade' or TRANSACAO_AMT_TRXN <> 0 then 'TRANSACAO_IMPRESSA'
            else 'DEFAULT1'
        end as ROUTE_GROUP
    from exp_trata_informacoes2
)

select
    TRANSACAO_CONTA,
    TRANSACAO_CARTAO,
    TRANSACAOI_TRXN_DATE,
    DESCR_1,
    TRANSACAO_DESCR_2,
    TRANSACAO_AMT_TRXN,
    ROUTE_GROUP
from rtr_separa_transacao