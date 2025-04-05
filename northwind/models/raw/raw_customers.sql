{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'customers') }}
)

SELECT * FROM source_data