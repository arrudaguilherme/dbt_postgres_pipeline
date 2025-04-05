{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    product_id AS id,
    category_id,
    supplier_id,
    product_name,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    units_on_order,
    reorder_level,
    "case"    
  FROM {{ref('stg_products') }}
)

SELECT * FROM source_data