{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'employee_territories') }}
)

SELECT * FROM source_data