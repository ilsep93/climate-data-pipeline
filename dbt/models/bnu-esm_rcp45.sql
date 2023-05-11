{{ 
  config(
    materialized="table"
) }}

select *
from {{ source('climatology', 'bnu-esm_rcp45') }}