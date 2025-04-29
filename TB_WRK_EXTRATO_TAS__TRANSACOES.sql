{{ config(materialized='table') }}

{% set table_check_query %}
    select 1
    from `{{this.schema}}.INFORMATION_SCHEMA.TABLES`
    where table_name = 'TB_PID_EXTRATO_TAS__TOTAIS_ASSUNTO_DIA_ETL'
{% endset %}

{% set table_exists = run_query(table_check_query) %}
{% set data_corte_default = "19900101" %}
{% set max_timestamp = '1990-01-01' %}

{% if table_exists and table_exists.rows|length > 0 %}
{% set max_date_query %}
    select
        format_date('%Y%m%d', coalesce(max(DT_TOTALASSUNETL), date '1990-01-01')) as max_data,
        timestamp(datetime(max(DT_INICARGA)), "America/Sao_Paulo") as max_ts
    from `{{ this.database }}.{{ this.schema }}.TB_PID_EXTRATO_TAS__TOTAIS_ASSUNTO_DIA_ETL`
{% endset %}
{% set max_data_result = run_query(max_date_query) %}
    {% if max_data_result and max_data_result.rows|length > 0 and max_data_result.columns[1].values()[0] is not none %}
        {% set data_corte_default = max_data_result.columns[0].values()[0] %}
        {% set max_timestamp = max_data_result.columns[1].values()[0] %}
    {% endif %}
{% endif %}


with origem_raw as (
    select
        regexp_extract(ingestion_source, r'[A-Z]{2}IssuerStatementFile[A-Z]+_\d+[A-Z]?_\d{8}') as nome_arquivo,
        ingestion_ref_date,
        ingestion_timestamp,
        tipo_registro,
        nm_conta,
        nm_cartao,
        data_transacao,
        desc_transacao,
        sinal_vlr_transacao,
        vlr_transacao_original,
    from {{ source('TESSERA_TRUSTED_EXTRATO_TAS', 'tsys_issuerstatement_transacoes') }}
    where ingestion_ref_date >= '{{ data_corte_default }}'
),

cte_final as (
    select
        substr(nome_arquivo, 1, 2) as PRODUTO,
        substr(nome_arquivo, 22, 4) as EMPRESA,
        nome_arquivo as NOME_ARQUIVO_RESUMIDO,
        ingestion_ref_date as DATA_CORTE,
        tipo_registro as TIPO_REGISTRO,
        nm_conta as NM_CONTA,
        nm_cartao as NM_CARTAO,
        data_transacao as DATA_TRANSACAO,
        desc_transacao as DESC_TRANSACAO,
        sinal_vlr_transacao as SINAL_VLR_TRANSACAO,
        vlr_transacao_original as VLR__TRANSACAO_ORIGINAL
    from origem_raw
    where ingestion_timestamp > timestamp('{{ max_timestamp }}')
)

select
    PRODUTO,
    EMPRESA,
    NOME_ARQUIVO_RESUMIDO,
    DATA_CORTE,
    TIPO_REGISTRO,
    NM_CONTA,
    NM_CARTAO,
    DATA_TRANSACAO,
    DESC_TRANSACAO,
    SINAL_VLR_TRANSACAO,
    VLR__TRANSACAO_ORIGINAL
from cte_final