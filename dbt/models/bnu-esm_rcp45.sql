select *
from {{ source('climatology', 'bnu-esm_rcp45') }}