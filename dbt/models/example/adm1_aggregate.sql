{{ config(materialized="table") }}
-- depends_on: {{ ref('table_1') }}
-- depends_on: {{ ref('table_2') }}
-- depends_on: {{ ref('table_3') }}
{% call statement('tables_for_union', fetch_result=True) %}
    SELECT table_name
    FROM `dbt-test.dbt_test.INFORMATION_SCHEMA.TABLES`
    WHERE table_name LIKE 'table_%'
{% endcall %}
{% set tables = load_result('tables_for_union')['data'] %}
{% for table in tables %}
SELECT *
FROM {{ ref(table[0]) }}
  {% if not loop.last %}
    UNION ALL
  {% endif %}
{% endfor %}