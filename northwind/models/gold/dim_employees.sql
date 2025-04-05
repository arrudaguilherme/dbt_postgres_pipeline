{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    employee_id AS id,
    first_name,
    last_name,
    title,
    title_of_courtesy,
    birth_date,
    hire_date,
    "address",
    city,
    region,
    postal_code,
    country,
    home_phone,
    extension,
    photo_path
  FROM {{ref('stg_employees') }}
)

SELECT * FROM source_data