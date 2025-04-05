{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
    SELECT
        CAST(region_id AS TEXT) AS region_id,
        TRIM(region_description) AS region_description
  FROM {{ref('raw_region')}}
)

SELECT * FROM source_data