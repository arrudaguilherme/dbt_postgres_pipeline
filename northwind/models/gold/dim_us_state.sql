{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    state_id AS id,
    state_name,
    state_abbr AS state_abbreviation,
    state_region
  FROM {{ref('stg_us_states') }}
)

