{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    order_id AS id,
    product_id,
    discount
  FROM {{ref('stg_order_details') }}
)

SELECT * FROM source_data