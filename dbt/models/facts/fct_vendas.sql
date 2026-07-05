{{ config(
    materialized='incremental',
    unique_key='venda_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

WITH vendas_base AS (
    SELECT
        v.id_vendas AS venda_id,
        v.id_veiculos AS veiculo_id,
        v.id_concessionarias AS concessionaria_id,
        v.id_vendedores AS vendedor_id,
        v.id_clientes AS cliente_id,
        v.valor_venda,
        v.data_venda,
        v.data_inclusao,
        v.data_atualizacao
    FROM
        {{ ref('stg_vendas') }} v
),
vendas AS (
    SELECT
        v.venda_id,
        v.veiculo_id,
        v.concessionaria_id,
        v.vendedor_id,
        v.cliente_id,
        v.valor_venda,
        v.data_venda,
        v.data_inclusao,
        v.data_atualizacao
    FROM
        vendas_base v
    LEFT JOIN
        {{ ref('dim_veiculos')}} vei
    ON
        vei.veiculo_id = v.veiculo_id
    LEFT JOIN
        {{ ref('dim_concessionarias')}} con 
    ON
        v.concessionaria_id = con.concessionaria_id
    LEFT JOIN
        {{ ref('dim_vendedores')}} ven
    ON  
        v.vendedor_id = ven.vendedor_id
    LEFT JOIN
        {{ ref('dim_clientes')}} cli
    ON
        v.cliente_id = cli.cliente_id
)
SELECT
    venda_id,
    veiculo_id,
    concessionaria_id,
    vendedor_id,
    cliente_id,
    valor_venda,
    data_venda,
    data_inclusao,
    data_atualizacao
FROM vendas
{% if is_incremental() %}
WHERE
    data_atualizacao >= (
        SELECT COALESCE(MAX(data_atualizacao), TO_TIMESTAMP_NTZ('1900-01-01 00:00:00'))
        FROM {{ this }}
    )
{% endif %}
