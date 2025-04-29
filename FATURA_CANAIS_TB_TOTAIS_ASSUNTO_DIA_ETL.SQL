{{
  config(materialized='table')
}}

with
cte_base as (
    select * from {{ ref('FATURA_CANAIS_WRK') }}
    where route_group = 'REJEITADO'
),

cte_agg as (
    select
        to_date(substr('{{ var("DATA_CORTE") }}',7,2)
        ||substr('{{ var("DATA_CORTE") }}',5,2)
        ||substr('{{ var("DATA_CORTE") }}',1,4), 'DDMMYYYY') as dt_totalassunetl,
        34 as id_assunnegocetl,
        'Fatura Sem Transação'
         ||' '||'{{ var("PRODUTO") }}'
         ||' '||replace('{{ var("EMPRESA") }}','_','')
         ||' '||substr('{{ var("DATA_CORTE") }}',7,2)
               ||substr('{{ var("DATA_CORTE") }}',5,2)
               ||substr('{{ var("DATA_CORTE") }}',1,4) as dl_resultgroupby,
        0 as qt_totalassundestetl,
        '{{ var("NOME_ARQUIVO") }}' as nm_arqori,
        '{{ var("PRODUTO") }}' as nm_prodfat,
        case
            when upper('{{ var("EMPRESA") }}') = 'CARF' then 'CARREFOUR'
            when upper('{{ var("EMPRESA") }}') = 'SAMS' then 'SAMS'
            else 'ATACADAO'
        end as nm_emprs,
        cast('{{ var("run_started_at") }}' as timestamp) as dt_iniciarga,
        cast('{{ var("run_started_at") }}' as timestamp) as dt_fimcarga,
        count(1) as count_per_group
    from cte_base
    group by
        dt_totalassunetl,
        id_assunnegocetl,
        dl_resultgroupby,
        qt_totalassundestetl,
        nm_arqori,
        nm_prodfat,
        nm_emprs,
        dt_iniciarga,
        dt_fimcarga
),

cte_final as (
    select
        dt_totalassunetl,
        id_assunnegocetl,
        dl_resultgroupby,
        sum(count_per_group) over (
            partition by dt_totalassunetl,
            id_assunnegocetl,
            dl_resultgroupby,
            qt_totalassundestetl,
            nm_arqori,
            nm_prodfat,
            nm_emprs,
            dt_iniciarga,
            dt_fimcarga
        ) as qt_totalassunorietl,
        qt_totalassundestetl,
        nm_arqori,
        nm_prodfat,
        nm_emprs,
        dt_iniciarga,
        dt_fimcarga
    from cte_agg
)

select distinct
    dt_totalassunetl,
    id_assunnegocetl,
    dl_resultgroupby,
    qt_totalassunorietl,
    qt_totalassundestetl,
    nm_arqori,
    nm_prodfat,
    nm_emprs,
    dt_iniciarga,
    dt_fimcarga
from cte_final;