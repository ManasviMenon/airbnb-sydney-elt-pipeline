{{ config(
    materialized='table',
    schema='silver',
    alias='lga_suburb_mapping'
) }}

WITH cleaned AS (
    SELECT
        TRIM(suburb_name) AS suburb_name,
        TRIM(lga_name) AS lga_name
    FROM bronze.lga_suburb_mapping
)

SELECT *
FROM cleaned
WHERE suburb_name IS NOT NULL
  AND suburb_name <> ''
  AND lga_name IS NOT NULL
  AND lga_name <> ''
