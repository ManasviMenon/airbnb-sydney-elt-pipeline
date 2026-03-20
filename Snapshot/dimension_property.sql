{% snapshot dimension_property %}
{{
  config(
    target_schema='snapshots',
    unique_key='property_id',
    strategy='check',
    check_cols=['property_type', 'room_type', 'accommodates']
  )
}}

WITH recent_properties AS (
    SELECT *
    FROM {{ ref('airbnb_raw') }}
    WHERE scraped_date >= (CURRENT_DATE - INTERVAL '12 months')
)

SELECT
    MD5(CONCAT(property_type, '|', room_type, '|', accommodates::TEXT)) AS property_id,
    COALESCE(NULLIF(LOWER(TRIM(property_type)), ''), 'unknown') AS property_type,
    COALESCE(NULLIF(LOWER(TRIM(room_type)), ''), 'unknown') AS room_type,
    COALESCE(accommodates::INT, 0) AS accommodates,
    scraped_date
FROM recent_properties
GROUP BY property_type, room_type, accommodates, scraped_date

{% endsnapshot %}
