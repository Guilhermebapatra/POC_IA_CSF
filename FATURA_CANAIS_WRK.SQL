{{
  config(materialized='table')
}}

with
cte_ff_boleto_pagamento_tsys as (
  select
    PRODUTO,
    EMPRESA,
    NOME_ARQUIVO_RESUMIDO,
    DATA_CORTE,
    trim(NM_CONTA) as NM_CONTA,
    trim(LINHA_IMPRIMIVEL) as LINHA_IMPRIMIVEL,
  from {{ ref("TB_WRK_EXTRATO_TAS__BOLETO_PAGAMENTO") }}
),

cte_ff_tb_extrato_detalhe as (
  select
    transacao_conta,
    transacao_cartao,
    TRANSACAOI_TRXN_DATE as transacao_trxn_date,
    descr_1,
    transacao_descr_2,
    transacao_amt_trxn
  from {{ ref("TB_WRK_EXTRATO_TAS__EXTRATO_DETALHE_TMP") }}
),

cte_ff_tb_extrato_resumido as (
  select
    cycle_date,
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
  from {{ ref("TB_WRK_EXTRATO_TAS__EXTRATO_RESUMIDO_TMP") }}
),

cte_ff_cliente_tsys as (
  select
    trim(cast(nm_conta as string)) as cliente_conta,
    trim(no_cpf) as cliente_cpf
  from {{ ref("TB_WRK_EXTRATO_TAS__CLIENTE") }}
),

cte_join_conta_cliente as (
  select
    res.cycle_date,
    res.conta_conta,
    res.billing_date_ini,
    res.billing_date_fim,
    res.conta_total_credits,
    res.conta_total_debits,
    res.conta_mindueamount,
    res.conta_total_paymetns,
    res.conta_opening_balance,
    res.conta_closing_balance,
    res.conta_instalment_amt,
    res.dt_carga,
    res.serno_ods,
    cli.cliente_cpf
  from cte_ff_tb_extrato_resumido as res
  inner join cte_ff_cliente_tsys as cli
    on cli.cliente_conta = res.conta_conta
),

cte_join_conta_cliente_boleto as (
  select
    cc.cycle_date,
    cc.conta_conta,
    cc.billing_date_ini,
    cc.billing_date_fim,
    cc.conta_total_credits,
    cc.conta_total_debits,
    cc.conta_mindueamount,
    cc.conta_total_paymetns,
    cc.conta_opening_balance,
    cc.conta_closing_balance,
    cc.conta_instalment_amt,
    cc.dt_carga,
    cc.serno_ods,
    cc.cliente_cpf,
    bol.linha_imprimivel as linha_digitavel
  from cte_join_conta_cliente as cc
  inner join cte_ff_boleto_pagamento_tsys as bol
    on bol.nm_conta = cc.conta_conta
),

cte_join_all as (
  select
    det.transacao_conta,
    det.transacao_cartao,
    det.transacao_trxn_date,
    det.descr_1,
    det.transacao_descr_2,
    det.transacao_amt_trxn,
    res.cycle_date,
    res.conta_conta,
    res.billing_date_ini,
    res.billing_date_fim,
    res.conta_total_credits,
    res.conta_total_debits,
    res.conta_mindueamount,
    res.conta_total_paymetns,
    res.conta_opening_balance,
    res.conta_closing_balance,
    res.conta_instalment_amt,
    res.dt_carga,
    res.serno_ods,
    res.cliente_cpf,
    res.linha_digitavel
  from cte_ff_tb_extrato_detalhe as det
  right outer join cte_join_conta_cliente_boleto as res
    on res.conta_conta = det.transacao_conta
),

cte_router as (
  select
    transacao_conta,
    transacao_cartao,
    transacao_trxn_date,
    descr_1,
    transacao_descr_2,
    transacao_amt_trxn,
    cycle_date,
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
    serno_ods,
    cliente_cpf,
    linha_digitavel,
    case
      when transacao_conta is null then 'REJEITADO'
      when transacao_conta is not null then 'RESUMIDO_DETALHE'
      else 'DEFAULT1'
    end as route_group
  from cte_join_all
)

select
  TRANSACAO_CONTA,
  TRANSACAO_CARTAO,
  TRANSACAO_TRXN_DATE,
  DESCR_1,
  TRANSACAO_DESCR_2,
  TRANSACAO_AMT_TRXN,
  CYCLE_DATE,
  CONTA_CONTA,
  BILLING_DATE_INI,
  BILLING_DATE_FIM,
  CONTA_TOTAL_CREDITS,
  CONTA_TOTAL_DEBITS,
  CONTA_MINDUEAMOUNT,
  CONTA_TOTAL_PAYMETNS,
  CONTA_OPENING_BALANCE,
  CONTA_CLOSING_BALANCE,
  CONTA_INSTALMENT_AMT,
  DT_CARGA,
  SERNO_ODS,
  CLIENTE_CPF,
  LINHA_DIGITAVEL,
  ROUTE_GROUP
from cte_router