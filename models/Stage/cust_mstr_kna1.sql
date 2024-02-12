with source_raw as (
 
    select * from {{ source('source_raw', 'cust_mstr_kna1') }}
 
),
 
cust_mstr_kna1 as (
 
    select
 
        cust_number as cust_number,
        cust_location as cust_location,
        cust_country as cust_country
 
    from source_raw
 
)
 
select * from cust_mstr_kna1