{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    territory_id AS id,
    territory_description,
    region_id,
    territory_description AS "description"
  FROM {{ref('stg_territories') }}
)

SELECT * FROM source_data