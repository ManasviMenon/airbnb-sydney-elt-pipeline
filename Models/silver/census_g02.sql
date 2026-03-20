{{ config(
    materialized='table',
    schema='silver',
    alias='census_g02'
) }}

-- Clean and standardize
WITH cleaned AS (
    SELECT
        TRIM(lga_code_2016) AS lga_code_2016,

        -- -- Replace negative values with NULL
        -- CASE WHEN median_age_persons >= 0 THEN median_age_persons ELSE NULL END AS median_age_persons,
        -- CASE WHEN median_mortgage_repay_monthly >= 0 THEN median_mortgage_repay_monthly ELSE NULL END AS median_mortgage_repay_monthly,
        -- CASE WHEN median_tot_prsnl_inc_weekly >= 0 THEN median_tot_prsnl_inc_weekly ELSE NULL END AS median_tot_prsnl_inc_weekly,
        -- CASE WHEN median_rent_weekly >= 0 THEN median_rent_weekly ELSE NULL END AS median_rent_weekly,
        -- CASE WHEN median_tot_fam_inc_weekly >= 0 THEN median_tot_fam_inc_weekly ELSE NULL END AS median_tot_fam_inc_weekly,
        -- CASE WHEN average_num_psns_per_bedroom >= 0 THEN average_num_psns_per_bedroom ELSE NULL END AS average_num_psns_per_bedroom,
        -- CASE WHEN median_tot_hhd_inc_weekly >= 0 THEN median_tot_hhd_inc_weekly ELSE NULL END AS median_tot_hhd_inc_weekly,
        -- CASE WHEN average_household_size >= 0 THEN average_household_size ELSE NULL END AS average_household_size,

        -- Fill NULLs with 0 for numeric columns
        COALESCE(median_age_persons,0) AS median_age_persons,
        COALESCE(median_mortgage_repay_monthly,0) AS median_mortgage_repay_monthly,
        COALESCE(median_tot_prsnl_inc_weekly,0) AS median_tot_prsnl_inc_weekly,
        COALESCE(median_rent_weekly,0) AS median_rent_weekly,
        COALESCE(median_tot_fam_inc_weekly,0) AS median_tot_fam_inc_weekly,
        COALESCE(average_num_psns_per_bedroom,0) AS average_num_psns_per_bedroom,
        COALESCE(median_tot_hhd_inc_weekly,0) AS median_tot_hhd_inc_weekly,
        COALESCE(average_household_size,0) AS average_household_size

    FROM bronze.census_g02
)

-- Filter invalid rows
SELECT *
FROM cleaned
WHERE lga_code_2016 IS NOT NULL
