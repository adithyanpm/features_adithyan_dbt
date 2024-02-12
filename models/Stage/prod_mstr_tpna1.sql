with source_raw as (
 
    select * from {{ source('source_raw', 'prod_mstr_tpna1') }}
 
),
 
prod_mstr_tpna1 as (
 
    select
 
        product_id as product_id,
        product_name as product_name
 
    from source_raw
 
)
 
select * from prod_mstr_tpna1