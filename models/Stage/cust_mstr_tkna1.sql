with source_raw as (
 
    select * from {{ source('source_raw', 'cust_mstr_tkna1') }}
 
),
 
cust_mstr_tkna1 as (
 
    select
 
        cust_number as cust_number,
        first_name as first_name,
        last_name as last_name
 
    from source_raw
 
)
 
select * from cust_mstr_tkna1