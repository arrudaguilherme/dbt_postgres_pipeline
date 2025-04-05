{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'suppliers') }}
)

SELECT * FROM source_data