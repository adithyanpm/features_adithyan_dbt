{{
    config(
        materialized='incremental',
        unique_key= ['hashkey']
    )
}}
 
with cust as (
    select * from {{ref("cust_mstr")}}
),
prod as (
    select * from {{ref("prod_mstr")}}
),
invoice as (
    select * from {{ref("invoice_raw")}}
),
final as (
    select
        date(i.transaction_timestamp) as transaction_date,
        c.cust_nbr,
        c.cust_nm,
        c.cust_loc,
        c.ctry_cd,
        p.prod_id,
        p.prod_nm,
        p.catg_cd,
        i.region,
        i.zone,
        sum(i.quantity) as total_quantity,
        sum(i.quantity*p.prod_pricing) as total_value,
        sum(((i.quantity*p.prod_pricing)*(p.prod_margin))/100) as total_margin,
        count(i.product_id) as total_records,
        i.creation_date as creation_date,
        i.hashkey
       
 
        from invoice as i
        left join cust as c
        on i.customer_nbr = c.cust_nbr
        left join prod as p
        on i.product_id = p.prod_id
        group by c.cust_nbr,c.cust_nm,c.cust_loc,
        c.ctry_cd,p.prod_id,p.prod_nm,p.catg_cd,i.region,
        i.zone,i.transaction_timestamp,i.creation_date,i.hashkey
)
 
select * from final
 
{% if is_incremental() %}
 
where
  transaction_date > (select max(transaction_date) from {{ this }})
 
{% endif %}