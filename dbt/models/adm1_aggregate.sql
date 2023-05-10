{{ 
  config(
    materialized="table"
) }}

{% set tables = ["access1-0_rcp45", "access1-0_rcp85", "bnu-esm_rcp26"] %}

{% for table in tables %}

  SELECT *
  FROM {{ ref(table) }}

  {% if not loop.last %}
    UNION ALL
  {% endif %}
{% endfor %}