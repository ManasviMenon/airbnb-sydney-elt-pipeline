{% snapshot dimension_roomtype %}
{{
  config(
    target_schema='snapshots',
    unique_key='room_type_id',
    strategy='check',
    check_cols=['room_type']
  )
}}

WITH recent_roomtypes AS (
    SELECT *
    FROM {{ ref('airbnb_raw') }}
    WHERE scraped_date >= (CURRENT_DATE - INTERVAL '12 months')
)

SELECT
    MD5(LOWER(TRIM(room_type))) AS room_type_id,
    COALESCE(NULLIF(LOWER(TRIM(room_type)), ''), 'unknown') AS room_type,
    scraped_date
FROM recent_roomtypes
GROUP BY room_type, scraped_date

{% endsnapshot %}
