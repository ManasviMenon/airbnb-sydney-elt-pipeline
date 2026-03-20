{{ config(materialized='table', schema='gold', alias='dim_property') }}

WITH src AS (
    SELECT
        property_id,
        property_type,
        room_type,
        accommodates,
        scraped_date
    FROM {{ ref('dimension_property') }}
),

with_ranges AS (
    SELECT
        property_id,
        property_type,
        room_type,
        accommodates,
        scraped_date AS effective_from,
        LEAD(scraped_date) OVER (PARTITION BY property_id ORDER BY scraped_date) AS effective_to
    FROM src
)

SELECT
    property_id,
    property_type,
    room_type,
    accommodates,
    effective_from,
    effective_to,
    CASE WHEN effective_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM with_ranges
