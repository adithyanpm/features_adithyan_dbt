{{
    config(
        materialized='table'
    )
}}
 
with day_invoice as(
    select * from {{ref('day_invoice')}}
),
 
final as (
    select
        month(di.transaction_date) as transaction_month,
        di.cust_nbr,
        di.cust_nm,
        di.cust_loc,
        di.ctry_cd,
        di.prod_id,
        di.prod_nm,
        di.catg_cd,
        di.region,
        di.zone,
        sum(di.total_quantity) as total_quantity,
        sum(di.total_value) as total_value,
        sum(di.total_margin) as total_margin,
        count(di.total_records) as total_records
 
        from day_invoice as di
        group by di.transaction_date,
        di.cust_nbr, di.cust_nm, di.cust_loc,
        di.ctry_cd, di.prod_id, di.prod_nm, di.catg_cd, di.region, di.zone
 
    )
 
 Select * from final