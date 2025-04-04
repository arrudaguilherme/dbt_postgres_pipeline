{{
  config(
    materialized='view',
    schema='raw',
  )
}}

WITH source_data AS (
  SELECT * 
  FROM {{source('public', 'categories') }}
)

SELECT * FROM source_data

