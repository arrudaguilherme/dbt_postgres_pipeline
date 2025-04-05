{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
    SELECT
        CAST(territory_id AS TEXT) AS territory_id,
        CAST(region_id AS TEXT) AS region_id,
        TRIM(territory_description) AS territory_description
  FROM {{ref('raw_territories')}}
)

SELECT * FROM source_data