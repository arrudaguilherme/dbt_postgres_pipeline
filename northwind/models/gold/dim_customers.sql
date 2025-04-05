{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    customer_id AS id,
    company_name,
    contact_name,
    contact_title,
    "address",
    city,
    region,
    postal_code,
    country,
    phone,
    fax
  FROM {{ref('stg_customers') }}
)

SELECT * FROM source_data