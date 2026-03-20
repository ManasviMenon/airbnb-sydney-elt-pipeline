{{ config(materialized='table', schema='gold', alias='dim_roomtype') }}

WITH src AS (
    SELECT
        room_type_id,
        room_type,
        scraped_date
    FROM {{ ref('dimension_roomtype') }}
),

with_ranges AS (
    SELECT
        room_type_id,
        room_type,
        scraped_date AS effective_from,
        LEAD(scraped_date) OVER (PARTITION BY room_type_id ORDER BY scraped_date) AS effective_to
    FROM src
)

SELECT
    room_type_id,
    room_type,
    effective_from,
    effective_to,
    CASE WHEN effective_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM with_ranges
