{{ config(materialized='table') }}

with cte_ff_conta_tsys as (
  select
      TIPO_REGISTRO
    , NM_CONTA
    , DATA_VENCIMENTO
    , VL_PAGAMENTO_MIN
    , SALDO_FECHADO
    , SLDO_CREDIARIO_PENDENTE
    , PAGAMENTO_TOTAL
    , DEBITO_TOTAL
    , CREDITO_TOTAL
    , SLDO_ABERTURA
    , DATA_CORTE
    , PRODUTO
    , EMPRESA
    , NOME_ARQUIVO_RESUMIDO
  from {{ ref('TB_WRK_EXTRATO_TAS__CONTA') }}
),

cte_tods_tsys_caccounts as (
  select distinct
    cast(COD_CONTA_ORIGEM as decimal) as COD_CONTA_ORIGEM,
    COD_CONTA
  from {{ ref('TB_FAT__CONTA_CARTAO_HIST') }}
  where TMP_FIM_VALIDADE is null
),

cte_exp_ff_conta_tsys as (
  select distinct
    TIPO_REGISTRO,
    NM_CONTA as NM_CONTA,
    format_date("%Y%m%d", DATA_VENCIMENTO) as DATA_VENCIMENTO,
    VL_PAGAMENTO_MIN,
    SALDO_FECHADO,
    SLDO_CREDIARIO_PENDENTE,
    PAGAMENTO_TOTAL,
    DEBITO_TOTAL,
    CREDITO_TOTAL,
    SLDO_ABERTURA,
    DATA_CORTE,
    PRODUTO,
    EMPRESA,
    NOME_ARQUIVO_RESUMIDO,
  from cte_ff_conta_tsys
),

cte_trata_informacoes as (
  select
    tipo_registro,
    substr(data_vencimento, 5, 4) || substr(data_vencimento, 3, 2) || substr(data_vencimento, 1, 2) as out_cycle_date,
    trim(nm_conta) as conta_conta,
    cast(format_date('%Y%m%d', date_sub(parse_date('%Y%m%d', cast(DATA_CORTE as string)), interval 45 day)) as string) as billing_date_ini,
    cast(DATA_CORTE as string) as billing_date_fim,
    cast(credito_total as decimal) as conta_total_credits,
    cast(debito_total as decimal) as conta_total_debits,
    cast(vl_pagamento_min as decimal) as conta_mindueamount,
    cast(pagamento_total as decimal) as conta_total_paymetns,
    cast(sldo_abertura as decimal) as conta_opening_balance,
    cast(saldo_fechado as decimal) as conta_closing_balance,
    cast(sldo_crediario_pendente as decimal) as conta_instalment_amt,
    timestamp(datetime(current_timestamp(), 'America/Sao_Paulo')) as dt_carga
  from cte_exp_ff_conta_tsys
),

cte_join_arquivo_ods as (
  select
    exp.out_cycle_date,
    exp.conta_conta,
    exp.billing_date_ini,
    exp.billing_date_fim,
    exp.conta_total_credits,
    exp.conta_total_debits,
    exp.conta_mindueamount,
    exp.conta_total_paymetns,
    exp.conta_opening_balance,
    exp.conta_closing_balance,
    exp.conta_instalment_amt,
    exp.dt_carga,
    src.COD_CONTA_ORIGEM as serno_ods
  from cte_trata_informacoes as exp
  left outer join cte_tods_tsys_caccounts as src
    on src.COD_CONTA = exp.conta_conta
),

cte_valida_fatura as (
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
    serno_ods,
    case
      when serno_ods is not null and (conta_closing_balance > 2.48 or conta_closing_balance < 0.01) then 'TOTAL_FATURAS_VALIDAS_CARREGAR'
      when conta_closing_balance >= 0.01 and conta_closing_balance <= 2.48 then 'SALDO_FATURA_001_248'
      when serno_ods is null then 'CONTA_NAO_CADASTRO_ODS'
      else 'TOTAL_FATURAS_ARQUIVOS'
    end as route_group
  from cte_join_arquivo_ods
)

select
  OUT_CYCLE_DATE,
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
  ROUTE_GROUP
from cte_valida_fatura