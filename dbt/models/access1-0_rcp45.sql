{{ 
  config(
    materialized="table"
) }}

select *
from {{ source('climatology', 'access1-0_rcp45') }}