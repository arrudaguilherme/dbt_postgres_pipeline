{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT * 
  FROM {{ref('stg_categories') }}
)

SELECT * FROM source_data 

