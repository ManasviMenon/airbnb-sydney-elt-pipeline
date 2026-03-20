{{ config(materialized='view', schema='gold', alias='dm_property_type') }}

WITH fact_with_host AS (
    SELECT
        f.*,
        h.host_id,
        h.host_is_superhost
    FROM {{ ref('fact') }} f
    LEFT JOIN {{ ref('dim_host') }} h
        ON f.dim_host_id = h.host_id AND h.is_current = TRUE
),

agg AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        DATE_TRUNC('month', scraped_date)::DATE AS month_start,
        COUNT(*) AS total_listings,
        SUM(CASE WHEN has_availability THEN 1 ELSE 0 END) AS active_listings,
        MIN(CASE WHEN has_availability THEN price END) AS min_price,
        MAX(CASE WHEN has_availability THEN price END) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability THEN price END) AS median_price,
        AVG(CASE WHEN has_availability THEN price END) AS avg_price,
        COUNT(DISTINCT CASE WHEN has_availability THEN host_id END) AS distinct_hosts,
        -- Safely calculate superhost_rate
        CASE WHEN COUNT(DISTINCT host_id) = 0 THEN 0
             ELSE SUM(CASE WHEN has_availability AND host_is_superhost THEN 1 ELSE 0 END)::FLOAT
                  / COUNT(DISTINCT host_id) * 100 END AS superhost_rate,
        AVG(CASE WHEN has_availability THEN review_scores_rating END) AS avg_review_score,
        SUM(CASE WHEN has_availability THEN 30 - availability_30 ELSE 0 END) AS total_stays,
        SUM(CASE WHEN has_availability THEN (30 - availability_30) * price ELSE 0 END) AS estimated_revenue
    FROM fact_with_host
    GROUP BY property_type, room_type, accommodates, month_start
),

pct_change AS (
    SELECT
        *,
        LAG(active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_start) AS prev_active,
        LAG(total_listings - active_listings) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_start) AS prev_inactive
    FROM agg
)

SELECT
    property_type,
    room_type,
    accommodates,
    month_start,
    -- Safely calculate active_listing_rate
    CASE WHEN total_listings = 0 THEN 0
         ELSE active_listings::FLOAT / total_listings * 100 END AS active_listing_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    distinct_hosts,
    superhost_rate,
    avg_review_score,
    -- Safely calculate percentage changes
    CASE WHEN prev_active IS NULL OR prev_active = 0 THEN NULL
         ELSE (active_listings - prev_active)::FLOAT / prev_active * 100 END AS pct_change_active,
    CASE WHEN prev_inactive IS NULL OR prev_inactive = 0 THEN NULL
         ELSE ((total_listings - active_listings) - prev_inactive)::FLOAT / prev_inactive * 100 END AS pct_change_inactive,
    total_stays,
    -- Safely calculate avg_estimated_revenue_per_active_listing
    CASE WHEN active_listings = 0 THEN 0
         ELSE estimated_revenue / active_listings END AS avg_estimated_revenue_per_active_listing
FROM pct_change
ORDER BY property_type, room_type, accommodates, month_start