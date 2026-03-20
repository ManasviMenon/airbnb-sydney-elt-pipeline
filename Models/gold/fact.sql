{{ config(materialized='table', schema='gold', alias='fact_listing') }}

WITH listings AS (
    SELECT
        listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating
    FROM {{ ref('airbnb_raw') }}
),

-- Join to current host dimension
dim_host_curr AS (
    SELECT *
    FROM {{ ref('dim_host') }}
    WHERE is_current = TRUE
),

-- Join to current property dimension
dim_property_curr AS (
    SELECT *
    FROM {{ ref('dim_property') }}
    WHERE is_current = TRUE
),

-- Join to current room type dimension
dim_roomtype_curr AS (
    SELECT *
    FROM {{ ref('dim_roomtype') }}
    WHERE is_current = TRUE
),

-- LGA mapping (static)
dim_lga_curr AS (
    SELECT *
    FROM {{ ref('dim_lga') }}
),

fact_enriched AS (
    SELECT
        l.listing_id,
        l.scrape_id,
        l.scraped_date,

        -- Dimension keys
        h.host_id AS dim_host_id,
        p.property_id AS dim_property_id,
        r.room_type_id AS dim_roomtype_id,
        md5(l.listing_neighbourhood || '|' || l.host_neighbourhood) AS dim_neighbourhood_id,

        -- Metrics
        l.price,
        l.has_availability,
        l.availability_30,
        l.number_of_reviews,
        l.review_scores_rating,

        -- Attributes
        l.property_type,
        l.room_type,
        l.accommodates,
        l.listing_neighbourhood,
        l.host_neighbourhood

    FROM listings l

    LEFT JOIN dim_host_curr h
        ON l.host_id = h.host_id

    LEFT JOIN dim_property_curr p
        ON md5(l.property_type || '|' || l.room_type || '|' || l.accommodates::text) = p.property_id

    LEFT JOIN dim_roomtype_curr r
        ON md5(l.room_type) = r.room_type_id

    LEFT JOIN dim_lga_curr n
        ON md5(l.listing_neighbourhood || '|' || l.host_neighbourhood) = n.lga_id
)

SELECT *
FROM fact_enriched
