{{ config(
    materialized='table',
    schema='silver',
    alias='airbnb_raw'
) }}

WITH cleaned AS (
    SELECT
        listing_id::BIGINT,
        scrape_id::BIGINT,
        -- If scraped_date is already DATE, no TO_DATE needed. Otherwise cast to text first
        scraped_date::DATE AS scraped_date,
        host_id::BIGINT,
        COALESCE(NULLIF(TRIM(LOWER(host_name)), ''), 'unknown') AS host_name,
        -- If host_since is already DATE, no TO_DATE needed. Otherwise cast to text first
        host_since::DATE AS host_since,
        -- Boolean fields: simple COALESCE
        COALESCE(host_is_superhost, FALSE) AS host_is_superhost,
        COALESCE(NULLIF(LOWER(TRIM(host_neighbourhood)), ''), 'unknown') AS host_neighbourhood,
        COALESCE(NULLIF(LOWER(TRIM(listing_neighbourhood)), ''), 'unknown') AS listing_neighbourhood,
        COALESCE(NULLIF(LOWER(TRIM(property_type)), ''), 'unknown') AS property_type,
        COALESCE(NULLIF(LOWER(TRIM(room_type)), ''), 'unknown') AS room_type,
        COALESCE(accommodates::INT, 0) AS accommodates,
        COALESCE(price::NUMERIC, 0) AS price,
        COALESCE(has_availability, FALSE) AS has_availability,
        COALESCE(availability_30::INT, 0) AS availability_30,
        COALESCE(number_of_reviews::INT, 0) AS number_of_reviews,
        COALESCE(review_scores_rating, 0) AS review_scores_rating,
        COALESCE(review_scores_accuracy, 0) AS review_scores_accuracy,
        COALESCE(review_scores_cleanliness, 0) AS review_scores_cleanliness,
        COALESCE(review_scores_checkin, 0) AS review_scores_checkin,
        COALESCE(review_scores_communication, 0) AS review_scores_communication,
        COALESCE(review_scores_value, 0) AS review_scores_value
    FROM bronze.airbnb_raw
)

SELECT *
FROM cleaned
