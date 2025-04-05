{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
    SELECT
        CAST(state_id AS TEXT) AS state_id,
        TRIM(state_name) AS state_name,
        TRIM(state_abbr) AS state_abbr,
        TRIM(state_region) AS state_region
  FROM {{ref('raw_us_states')}}
)

SELECT * FROM source_data