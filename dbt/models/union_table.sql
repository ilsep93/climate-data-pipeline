{{ config(materialized="table") }}

-- depends_on: {{ ref('access1-0_rcp45') }}
-- depends_on: {{ ref('access1-0_rcp85') }}
-- depends_on: {{ ref('bnu-esm_rcp26') }}
-- depends_on: {{ ref('bnu-esm_rcp45') }}
-- depends_on: {{ ref('ccsm4_rcp60') }}

{% call statement('tables_for_union', fetch_result=True) %}
    SELECT table_name 
    FROM    information_schema.tables
    WHERE table_schema='climatology'
{% endcall %}

{% set tables = load_result('tables_for_union')['data'] %}

{% for table in tables %}

SELECT *
FROM {{ ref(table[0]) }}

  {% if not loop.last %}
    UNION ALL
  {% endif %}
{% endfor %}