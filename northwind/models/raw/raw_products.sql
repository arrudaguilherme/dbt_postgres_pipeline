{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'products') }}
)

SELECT * FROM source_data