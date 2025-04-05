{{config(
  materialized='table'
)
}}

WITH source_data AS (
  SELECT 
    employee_id AS id,
    territory_id
  FROM {{ref('stg_employee_territories') }}
)

SELECT * FROM source_data