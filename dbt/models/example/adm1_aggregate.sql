{{ config(materialized='table') }}

-- Convert temperature to Kelvin

-- Create new table with ADM0 aggregate

SELECT min, max, mean, median
FROM {{ source('your_source', 'existed_table_name') }}
GROUP BY admin1Name