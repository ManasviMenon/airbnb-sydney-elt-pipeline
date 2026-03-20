{{ config(
    materialized='table',
    schema='silver',
    alias='lga_codes'
) }}

WITH cleaned AS (
    SELECT
        TRIM(lga_code) AS lga_code,
        TRIM(lga_name) AS lga_name
    FROM bronze.lga_codes
)

SELECT *
FROM cleaned
WHERE lga_code IS NOT NULL
  AND lga_code <> ''
