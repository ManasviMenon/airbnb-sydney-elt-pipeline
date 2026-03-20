{{ config(materialized='table', schema='gold', alias='dim_lga') }}

SELECT
    md5(lga_code) AS lga_id,
    lga_code,
    lga_name
FROM {{ ref('nsw_lga_codes') }}
WHERE lga_code IS NOT NULL
