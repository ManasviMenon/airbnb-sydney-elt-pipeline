{{ config(materialized='table') }}
select * from bronze.lga_codes