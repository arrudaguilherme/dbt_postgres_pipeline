{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    supplier_id AS id,
    supplier_name,
    contact_name,
    contact_title,
    "address",
    city,
    region,
    postal_code,
    country,
    phone,
    fax,
    home_page
  FROM {{ref('stg_suppliers') }}
)

SELECT * FROM source_data