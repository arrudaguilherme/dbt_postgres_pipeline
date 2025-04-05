{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
  SELECT 
    CAST(employee_id AS text) AS employee_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    TRIM(title) AS title,
    TRIM(title_of_courtesy) AS title_of_courtesy,
    birth_date::DATE,
    hire_date::DATE,
    TRIM("address") AS "address",
    TRIM(city) AS city,
    TRIM(region) AS region,
    TRIM(postal_code) AS postal_code,
    TRIM(country) AS country,
    TRIM(home_phone) AS home_phone,
    TRIM(extension) AS extension,
    TRIM(photo_path) AS photo_path
  FROM {{ref("raw_employees") }}
)

SELECT * FROM source_data