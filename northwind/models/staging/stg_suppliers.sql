{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
    SELECT
        CAST(supplier_id AS TEXT) AS supplier_id,
        TRIM(company_name) AS supplier_name,
        TRIM(contact_name) AS contact_name,
        TRIM(contact_title) AS contact_title,
        TRIM("address") AS "address",
        TRIM(city) AS city,
        TRIM(region) AS region,
        TRIM(postal_code) AS postal_code,
        TRIM(country) AS country,
        TRIM(phone) AS phone,
        TRIM(fax) AS fax,
        TRIM("homepage") AS home_page
  FROM {{ref('raw_suppliers')}}
)

SELECT * FROM source_data