{{ 
  config(
    materialized="table"
) }}

select *
from {{ source('climatology', 'ccsm4_rcp60') }}