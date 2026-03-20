{% snapshot dimension_host %}
{{
  config(
    target_schema='snapshots',
    unique_key='host_id',
    strategy='check',
    check_cols=['host_name', 'host_since', 'host_is_superhost', 'host_neighbourhood']
  )
}}

WITH recent_hosts AS (
    SELECT *
    FROM {{ ref('airbnb_raw') }}
    WHERE scraped_date >= (CURRENT_DATE - INTERVAL '12 months')
)

SELECT
    host_id,
    COALESCE(NULLIF(LOWER(TRIM(host_name)), ''), 'unknown') AS host_name,
    host_since,
    COALESCE(host_is_superhost, FALSE) AS host_is_superhost,
    COALESCE(NULLIF(LOWER(TRIM(host_neighbourhood)), ''), 'unknown') AS host_neighbourhood,
    scraped_date
FROM recent_hosts

{% endsnapshot %}
