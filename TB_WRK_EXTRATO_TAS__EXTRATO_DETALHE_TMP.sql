{{
  config(
    materialized='table'
  )
}}

with wrk as (
  select * from {{ ref('TB_WRK_EXTRATO_TAS__TRANSACAO_ROUTER') }}
),

filtered as (
  select
   TRANSACAO_CONTA,
   TRANSACAO_CARTAO,
   TRANSACAOI_TRXN_DATE,
   DESCR_1,
   TRANSACAO_DESCR_2,
   TRANSACAO_AMT_TRXN
  from wrk
  where route_group = 'TRANSACAO_IMPRESSA'
)

select
   TRANSACAO_CONTA,
   TRANSACAO_CARTAO,
   TRANSACAOI_TRXN_DATE,
   DESCR_1,
   TRANSACAO_DESCR_2,
   TRANSACAO_AMT_TRXN
from filtered