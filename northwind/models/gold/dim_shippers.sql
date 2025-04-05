{{config(
  materialized='table',
  unique_key='id',
)}}

WITH source_data AS (
  SELECT 
    shipper_id AS id,
    shipper_name,
    shipper_phone AS "phone"   
  FROM {{ref('stg_shippers') }}
)

SELECT * FROM source_data