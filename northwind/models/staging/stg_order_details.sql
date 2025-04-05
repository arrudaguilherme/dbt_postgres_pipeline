{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
  SELECT 
    CAST(order_id AS text) AS order_id,
    CAST(product_id AS text) AS product_id,
    CAST(unit_price AS REAL) AS unit_price,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(discount AS REAL) AS discount
  FROM {{ref("raw_order_details") }}
)

SELECT * FROM source_data