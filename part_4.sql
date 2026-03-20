--Question a) What are the demographic differences (e.g., age group distribution, household size) between the top 3 performing and lowest 3 performing LGAs based on estimated revenue per active listing over the last 12 months?
WITH ranked_lgas AS (
    SELECT 
        dl.lga_code,
        dl.lga_name,
        AVG(dln.avg_estimated_revenue_per_active_listing) AS avg_revenue,
        ROW_NUMBER() OVER (ORDER BY AVG(dln.avg_estimated_revenue_per_active_listing) DESC) AS revenue_rank_desc,
        ROW_NUMBER() OVER (ORDER BY AVG(dln.avg_estimated_revenue_per_active_listing) ASC) AS revenue_rank_asc
    FROM postgres_gold.dm_listing_neighbourhood dln
    JOIN postgres_gold.dim_lga dl 
      ON LOWER(TRIM(dln.listing_neighbourhood)) = LOWER(TRIM(dl.lga_name))
    WHERE dln.month_year BETWEEN '2021-01-01' AND '2021-12-01'
    GROUP BY dl.lga_code, dl.lga_name
),
selected_lgas AS (
    SELECT lga_code, lga_name, 'Top 3 Performers' AS category
    FROM ranked_lgas
    WHERE revenue_rank_desc <= 3
    UNION ALL
    SELECT lga_code, lga_name, 'Bottom 3 Performers' AS category
    FROM ranked_lgas
    WHERE revenue_rank_asc <= 3
)
SELECT 
    s.category,
    s.lga_name,
    g01.age_25_34_yr_p,
    g01.age_65_74_yr_p,
    g01.lang_spoken_home_eng_only_p,
    g01.lang_spoken_home_oth_lang_p,
    g02.average_household_size,
    g02.median_tot_prsnl_inc_weekly
FROM selected_lgas s
LEFT JOIN postgres_silver.census_g01 g01 
  ON CONCAT('LGA', s.lga_code) = g01.lga_code_2016
LEFT JOIN postgres_silver.census_g02 g02 
  ON CONCAT('LGA', s.lga_code) = g02.lga_code_2016
ORDER BY s.category, s.lga_name;


--Question b) Is there a correlation between the median age of a neighborhood (from Census data) and the revenue generated per active listing in that neighborhood?
WITH lga_revenue AS (
    SELECT 
        dl.lga_code,
        dl.lga_name,
        AVG((f.price * (30 - f.availability_30)) * 12) AS avg_estimated_revenue_per_active_listing
    FROM postgres_gold.fact_listing f
    JOIN postgres_gold.dim_lga dl 
      ON LOWER(TRIM(f.listing_neighbourhood)) = LOWER(TRIM(dl.lga_name))
    WHERE f.has_availability = TRUE
      AND f.price > 0
      AND f.scraped_date BETWEEN '2021-01-01' AND '2021-12-31'
    GROUP BY dl.lga_code, dl.lga_name
)
SELECT 
    corr(c.median_age_persons::double precision,
         r.avg_estimated_revenue_per_active_listing::double precision) AS correlation_coefficient
FROM lga_revenue r
JOIN postgres_silver.census_g02 c
  ON CONCAT('LGA', r.lga_code) = c.lga_code_2016;

--Question c) What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?
WITH neighbourhood_revenue AS (
    SELECT
        f.listing_neighbourhood,
        AVG((f.price * (30 - f.availability_30)) * 12) AS est_annual_revenue_per_listing
    FROM postgres_gold.fact_listing f
    WHERE f.price > 0
      AND f.has_availability = TRUE
      AND f.scraped_date BETWEEN '2021-01-01' AND '2021-12-31'
    GROUP BY f.listing_neighbourhood
),
top5_neighbourhoods AS (
    SELECT listing_neighbourhood
    FROM neighbourhood_revenue
    ORDER BY est_annual_revenue_per_listing DESC
    LIMIT 5
),
listing_stats AS (
    SELECT
        f.listing_neighbourhood,
        f.property_type,
        f.room_type,
        f.accommodates,
        COUNT(*) AS num_listings,
        SUM(30 - f.availability_30) AS total_stay_nights
    FROM postgres_gold.fact_listing f
    WHERE f.listing_neighbourhood IN (SELECT listing_neighbourhood FROM top5_neighbourhoods)
      AND f.price > 0
      AND f.has_availability = TRUE
      AND f.scraped_date BETWEEN '2021-01-01' AND '2021-12-31'
    GROUP BY f.listing_neighbourhood, f.property_type, f.room_type, f.accommodates
)
SELECT
    ls.listing_neighbourhood,
    ls.property_type,
    ls.room_type,
    ls.accommodates,
    ls.num_listings,
    ls.total_stay_nights
FROM listing_stats ls
JOIN (
    SELECT listing_neighbourhood, MAX(total_stay_nights) AS max_stays
    FROM listing_stats
    GROUP BY listing_neighbourhood
) ms
  ON ls.listing_neighbourhood = ms.listing_neighbourhood
 AND ls.total_stay_nights = ms.max_stays
ORDER BY ls.total_stay_nights DESC, ls.listing_neighbourhood;

--Question d)For hosts with multiple listings, are their properties concentrated within the same LGA, or are they distributed across different LGAs? 
WITH host_lga_summary AS (
    SELECT
        fl.dim_host_id AS host_id,
        COUNT(DISTINCT dl.lga_name) AS distinct_lgas,
        COUNT(fl.listing_id) AS total_listings
    FROM postgres_gold.fact_listing fl
    JOIN postgres_gold.dim_lga dl
      ON LOWER(TRIM(fl.listing_neighbourhood)) = LOWER(TRIM(dl.lga_name))
    WHERE fl.price > 0
      AND fl.has_availability = TRUE
    GROUP BY fl.dim_host_id
),
host_classification AS (
    SELECT
        host_id,
        total_listings,
        distinct_lgas,
        CASE
            WHEN distinct_lgas = 1 THEN 'Concentrated in one LGA'
            ELSE 'Distributed across multiple LGAs'
        END AS distribution_type
    FROM host_lga_summary
    WHERE total_listings > 1
)
SELECT
    distribution_type,
    COUNT(host_id) AS num_hosts,
    ROUND(COUNT(host_id) * 100.0 / SUM(COUNT(host_id)) OVER (), 2) AS percent_of_multi_listing_hosts
FROM host_classification
GROUP BY distribution_type
ORDER BY num_hosts DESC;

--Question e)For hosts with a single Airbnb listing, does the estimated revenue over the last 12 months cover the annualized median mortgage repayment in the corresponding LGA? Which LGA has the highest percentage of hosts that can cover it?
WITH SingleListingHosts AS (
    SELECT host_id, MIN(listing_id) AS listing_id
    FROM bronze.airbnb_raw
    GROUP BY host_id
    HAVING COUNT(*) = 1
),
HostInfo AS (
    SELECT
        s.host_id,
        a.host_neighbourhood,
        a.price,
        a.availability_30
    FROM
        SingleListingHosts s
    JOIN bronze.airbnb_raw a ON s.listing_id = a.listing_id
),
HostWithLGA AS (
    SELECT
        hi.host_id,
        hi.price,
        hi.availability_30,
        m.lga_name
    FROM
        HostInfo hi
    LEFT JOIN bronze.lga_suburb_mapping m 
        ON LOWER(TRIM(hi.host_neighbourhood)) = LOWER(TRIM(m.suburb_name))
),
HostMortgage AS (
    SELECT
        hwl.host_id,
        hwl.lga_name,
        hwl.price,
        hwl.availability_30,
        c.median_mortgage_repay_monthly * 12 AS median_mortgage_annual,
        (hwl.price * (30 - hwl.availability_30) * 12) AS estimated_annual_revenue,
        CASE 
            WHEN (hwl.price * (30 - hwl.availability_30) * 12) >= c.median_mortgage_repay_monthly * 12 THEN 1 ELSE 0 
        END AS covers_mortgage
    FROM
        HostWithLGA hwl
    LEFT JOIN bronze.census_g02 c 
        ON hwl.lga_name = c.lga_code_2016
)
SELECT
    lga_name,
    COUNT(host_id) AS total_hosts,
    SUM(covers_mortgage) AS hosts_covering_mortgage,
    ROUND( (SUM(covers_mortgage)::decimal / COUNT(host_id)) * 100, 2) AS percentage_covering
FROM
    HostMortgage
WHERE
    lga_name IS NOT NULL
GROUP BY
    lga_name
ORDER BY
    percentage_covering DESC
LIMIT 5;
