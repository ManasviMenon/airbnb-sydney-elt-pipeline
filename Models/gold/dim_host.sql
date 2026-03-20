{{ config(materialized='table', schema='gold', alias='dim_host') }}

WITH src AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        scraped_date
    FROM {{ ref('dimension_host') }}
),

with_ranges AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        scraped_date AS effective_from,
        LEAD(scraped_date) OVER (PARTITION BY host_id ORDER BY scraped_date) AS effective_to
    FROM src
)

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    effective_from,
    effective_to,
    CASE WHEN effective_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM with_ranges
