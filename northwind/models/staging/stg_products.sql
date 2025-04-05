{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
  SELECT 
    CAST(product_id AS TEXT) AS product_id,
    CAST(category_id AS TEXT) AS category_id,
    CAST(supplier_id AS TEXT) AS supplier_id,
    TRIM(product_name) AS product_name,
    TRIM(quantity_per_unit) AS quantity_per_unit,
    CAST(unit_price AS REAL) AS unit_price,
    CAST(units_in_stock AS INTEGER) AS units_in_stock,
    CAST(units_on_order AS INTEGER) AS units_on_order,
    CAST(reorder_level AS INTEGER) AS reorder_level,
    CASE
      WHEN discontinued = '1' THEN 'DISCONTINUED'
      WHEN discontinued = '0' THEN 'NOT DISCONTINUED'
      ELSE 
	  	NULL
    END
  FROM {{ref('raw_products')}}
)

SELECT * FROM source_data