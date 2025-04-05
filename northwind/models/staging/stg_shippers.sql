{{
  config(
    materialized='view'
  )
}}

WITH source_data AS (
    SELECT
        CAST(shipper_id AS TEXT) AS shipper_id,
        TRIM(company_name) AS shipper_name,
        TRIM(phone) AS shipper_phone
  FROM {{ref('raw_shippers')}}
)

SELECT * FROM source_data