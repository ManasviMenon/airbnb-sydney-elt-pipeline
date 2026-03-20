{{ config(materialized='table') }}
select * from bronze.airbnb_raw 
