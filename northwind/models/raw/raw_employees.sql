{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'employees') }}
)

SELECT * FROM source_data