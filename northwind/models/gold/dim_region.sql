{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    region_id AS id,
    region_description AS "description"
  FROM {{ref('stg_region') }}
)

SELECT * FROM source_data