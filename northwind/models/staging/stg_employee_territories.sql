{{
  config(
    materialized='view',
  )
}}

WITH source_data AS (
  SELECT 
    CAST(employee_id AS text) AS employee_id,
    CAST(territory_id AS text) AS territory_id
  FROM {{ref('raw_employee_territories') }}
)

SELECT * FROM source_data