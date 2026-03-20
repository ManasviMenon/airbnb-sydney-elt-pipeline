{{ config(materialized='view', schema='gold', alias='dm_listing_neighbourhood') }}

WITH base AS (
    SELECT
        f.listing_id,
        h.host_id,
        h.host_is_superhost,
        f.listing_neighbourhood,
        f.price,
        f.has_availability,
        f.availability_30,
        f.scraped_date,
        f.review_scores_rating
    FROM {{ ref('fact') }} f
    LEFT JOIN {{ ref('dim_host') }} h
        ON f.dim_host_id = h.host_id AND h.is_current = TRUE
),

aggregated AS (
    SELECT
        listing_neighbourhood,
        TO_CHAR(DATE_TRUNC('month', scraped_date), 'YYYY-MM') AS month_year,
        COUNT(DISTINCT listing_id) AS total_listings,
        COUNT(DISTINCT CASE WHEN has_availability THEN listing_id END) AS active_listings,
        COUNT(DISTINCT CASE WHEN NOT has_availability THEN listing_id END) AS inactive_listings,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN host_is_superhost THEN host_id END) AS superhosts,
        AVG(review_scores_rating) FILTER (WHERE has_availability) AS avg_review_score,
        MIN(price) FILTER (WHERE has_availability) AS min_price,
        MAX(price) FILTER (WHERE has_availability) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE has_availability) AS median_price,
        AVG(price) FILTER (WHERE has_availability) AS avg_price,
        SUM(30 - availability_30) AS total_stays,
        AVG((30 - availability_30) * price) FILTER (WHERE has_availability) AS avg_estimated_revenue_per_active_listing
    FROM base
    GROUP BY listing_neighbourhood, month_year
),

final AS (
    SELECT
        a.listing_neighbourhood,
        a.month_year,
        ROUND(((a.active_listings::NUMERIC / NULLIF(a.total_listings,0)) * 100), 2) AS active_listings_rate,
        a.min_price,
        a.max_price,
        a.median_price,
        a.avg_price,
        a.distinct_hosts,
        ROUND(((a.superhosts::NUMERIC / NULLIF(a.distinct_hosts,0)) * 100), 2) AS superhost_rate,
        a.avg_review_score,
        ROUND((( (a.active_listings - LAG(a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_year)) 
               / NULLIF(LAG(a.active_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_year),0) ) * 100)::NUMERIC, 2) AS pct_change_active_listings,
        ROUND((( (a.inactive_listings - LAG(a.inactive_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_year)) 
               / NULLIF(LAG(a.inactive_listings) OVER (PARTITION BY a.listing_neighbourhood ORDER BY a.month_year),0) ) * 100)::NUMERIC, 2) AS pct_change_inactive_listings,
        a.total_stays,
        a.avg_estimated_revenue_per_active_listing
    FROM aggregated a
)

SELECT *
FROM final
ORDER BY listing_neighbourhood, month_year