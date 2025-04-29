{ config(materialized='table') }

with wrk as (
  select * from {{ ref('FATURA_CANAIS_VALIDA_FATURA_WRK') }}
),

agg_total_faturas_arquivo_origem as (
  select
    parse_date('%Y%m%d', { var("DATA_CORTE") }) as dt_totalassunetl,
    73 as id_assunnegocetl,
    'Fatura APP, Portal e TAS - ' || ' ' || { var("PRODUTO") } || ' ' || replace({ var("EMPRESA") }, '_', '') || ' ' || substr({ var("DATA_CORTE") }, 7, 2) || substr({ var("DATA_CORTE") }, 5, 2) || substr({ var("DATA_CORTE") }, 1, 4) as dl_resultgroupby,
    count(1) as qt_totalassunorietl,
    0 as qt_totalassundestetl,
    { var("NOME_ARQUIVO") } as nm_arqori,
    { var("PRODUTO") } as nm_prodfat,
    case
      when { var("EMPRESA") } = 'CARF' then 'CARREFOUR'
      when { var("EMPRESA") } = 'SAM_' then 'SAMS'
      else 'ATACADAO'
    end as nm_emprs,
    current_timestamp() as dt_inicarga,
    current_timestamp() as dt_fimcarga
  from wrk
  where route_group = 'TOTAL_FATURAS_ARQUIVOS'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

agg_conta_nao_cadastrada_ods as (
  select
    parse_date('%Y%m%d', { var("DATA_CORTE") }) as dt_totalassunetl,
    cast(33 as numeric) as id_assunnegocetl,
    'Conta Não Encontrada no ODS' || ' ' || { var("PRODUTO") } || ' ' || replace({ var("EMPRESA") }, '_', '') || ' ' || substr({ var("DATA_CORTE") }, 7, 2) || substr({ var("DATA_CORTE") }, 5, 2) || substr({ var("DATA_CORTE") }, 1, 4) as dl_resultgroupby,
    count(1) as qt_totalassunorietl,
    cast(0 as numeric) as qt_totalassundestetl,
    { var("NOME_ARQUIVO") } as nm_arqori,
    { var("PRODUTO") } as nm_prodfat,
    case
      when { var("EMPRESA") } = 'CARF' then 'CARREFOUR'
      when { var("EMPRESA") } = 'SAM_' then 'SAMS'
      else 'ATACADAO'
    end as nm_emprs,
    current_timestamp() as dt_inicarga,
    current_timestamp() as dt_fimcarga
  from wrk
  where route_group = 'CONTA_NAO_CADASTRO_ODS'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

agg_saldo_fatura_001_248 as (
  select
    parse_date('%Y%m%d', { var("DATA_CORTE") }) as dt_totalassunetl,
    35 as id_assunnegocetl,
    'Saldo Fatura Entre 0.01 e 2.48' || ' ' || { var("PRODUTO") } || ' ' || replace({ var("EMPRESA") }, '_', '') || ' ' || substr({ var("DATA_CORTE") }, 7, 2) || substr({ var("DATA_CORTE") }, 5, 2) || substr({ var("DATA_CORTE") }, 1, 4) as dl_resultgroupby,
    count(1) as qt_totalassunorietl,
    0 as qt_totalassundestetl,
    { var("NOME_ARQUIVO") } as nm_arqori,
    { var("PRODUTO") } as nm_prodfat,
    case
      when { var("EMPRESA") } = 'CARF' then 'CARREFOUR'
      when { var("EMPRESA") } = 'SAM_' then 'SAMS'
      else 'ATACADAO'
    end as nm_emprs,
    current_timestamp() as dt_inicarga,
    current_timestamp() as dt_fimcarga
  from wrk
  where route_group = 'SALDO_FATURA_001_248'
  group by 1, 2, 3, 5, 6, 7, 8, 9, 10
),

unioned_data as (
  select * from agg_total_faturas_arquivo_origem
  union all
  select * from agg_conta_nao_cadastrada_ods
  union all
  select * from agg_saldo_fatura_001_248
)

select
  dt_totalassunetl,
  id_assunnegocetl,
  dl_resultgroupby,
  qt_totalassunorietl,
  qt_totalassundestetl,
  nm_arqori,
  nm_prodfat,
  nm_emprs,
  dt_inicarga,
  dt_fimcarga
from unioned_data;