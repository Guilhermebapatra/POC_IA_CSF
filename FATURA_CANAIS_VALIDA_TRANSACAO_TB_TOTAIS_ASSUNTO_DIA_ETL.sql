{{ config(materialized='table') }}

with base_data as (
    select
        ROUTE_GROUP
    from {{ ref('FATURA_CANAIS_VALIDA_TRANSACAO_WRK') }}
    where ROUTE_GROUP = 'TRANSACAO_REJEITADA_VALOR_ZERADO'
),

agg_transacao_zerada as (
    select
        parse_date('%d%m%Y', {{ var("DATA_CORTE") }}) as DT_TOTALASSUNETL,
        74 as ID_ASSUNNEGOCETL,
        'Transação Valor Zerada ' || {{ var("PRODUTO") }} || ' ' || replace({{ var("EMPRESA") }}, '_', '') || ' ' || {{ var("DATA_CORTE") }} as DL_RESULTGROUPBY,
        count(1) as QT_TOTALASSUNORIETL,
        0 as QT_TOTALASSUNDESTETL,
        {{ var("NOME_ARQUIVO") }} as NM_ARQORI,
        {{ var("PRODUTO") }} as NM_PRODFAT,
        case
            when {{ var("EMPRESA") }} = 'CARF' then 'CARREFOUR'
            when {{ var("EMPRESA") }} = 'SAM_' then 'SAMS'
            else 'ATACADAO'
        end as NM_EMPRS,
        current_timestamp() as DT_INICARGA,
        current_timestamp() as DT_FIMCARGA
    from base_data
    group by
        DT_TOTALASSUNETL,
        ID_ASSUNNEGOCETL,
        DL_RESULTGROUPBY,
        QT_TOTALASSUNDESTETL,
        NM_ARQORI,
        NM_PRODFAT,
        NM_EMPRS,
        DT_INICARGA,
        DT_FIMCARGA
)

select
    DT_TOTALASSUNETL,
    ID_ASSUNNEGOCETL,
    DL_RESULTGROUPBY,
    QT_TOTALASSUNORIETL,
    QT_TOTALASSUNDESTETL,
    NM_ARQORI,
    NM_PRODFAT,
    NM_EMPRS,
    DT_INICARGA,
    DT_FIMCARGA
from agg_transacao_zerada