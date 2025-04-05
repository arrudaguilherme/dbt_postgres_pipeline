{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
  SELECT 
    CAST(category_id AS text) AS category_id,
    TRIM(category_name) AS name,
    TRIM(description) AS description,
    picture
  FROM {{ref("raw_categories") }}
)

SELECT * FROM source_data