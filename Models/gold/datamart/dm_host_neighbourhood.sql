{{ config(materialized='view', schema='gold', alias='dm_host_neighbourhood') }}

WITH fact_with_host AS (
    SELECT
        f.*,
        h.host_id,
        h.host_is_superhost,
        l.lga_name AS host_neighbourhood_lga
    FROM {{ ref('fact') }} f
    LEFT JOIN {{ ref('dim_host') }} h
        ON f.dim_host_id = h.host_id AND h.is_current = TRUE
    LEFT JOIN {{ ref('dim_lga') }} l
        ON LOWER(TRIM(f.host_neighbourhood)) = LOWER(TRIM(l.lga_name))
),

agg AS (
    SELECT
        host_neighbourhood_lga,
        TO_CHAR(DATE_TRUNC('month', scraped_date), 'YYYY-MM') AS year_month,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE 0 END) AS estimated_revenue,
        SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) AS active_listings_count
    FROM fact_with_host
    GROUP BY host_neighbourhood_lga, year_month
)

SELECT
    host_neighbourhood_lga,
    year_month,
    distinct_hosts,
    CASE WHEN active_listings_count = 0 THEN 0
         ELSE estimated_revenue / active_listings_count END AS estimated_revenue_per_active_listing,
    CASE WHEN distinct_hosts = 0 THEN 0
         ELSE estimated_revenue / distinct_hosts END AS estimated_revenue_per_host
FROM agg
ORDER BY host_neighbourhood_lga, year_month