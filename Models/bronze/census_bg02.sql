{{ config(materialized='table') }}
select * from bronze.census_g02