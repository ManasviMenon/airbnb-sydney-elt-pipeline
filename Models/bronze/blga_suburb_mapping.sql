{{ config(materialized='table') }}
select * from bronze.lga_suburb_mapping