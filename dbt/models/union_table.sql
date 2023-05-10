{{ config(materialized="table") }}

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