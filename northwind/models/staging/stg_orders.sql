{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
  SELECT 
    CAST(order_id AS text) AS order_id,
    CAST(customer_id AS text) AS customer_id,
    CAST(employee_id AS text) AS employee_id,
    order_date::DATE,
    required_date::DATE,
    shipped_date::DATE,
    CAST(ship_via AS text) AS ship_via,
    CAST(freight AS real) AS freight,
    TRIM(ship_name) AS ship_name,
    TRIM(ship_address) AS ship_address,
    TRIM(ship_city) AS ship_city,
    TRIM(ship_region) AS ship_region,
    TRIM(ship_postal_code) AS ship_postal_code,
    TRIM(ship_country) AS ship_country
  FROM {{ref("raw_orders") }}
)

SELECT * FROM source_data