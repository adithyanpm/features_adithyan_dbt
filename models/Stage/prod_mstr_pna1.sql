with source_raw as (
 
    select * from {{ source('source_raw', 'prod_mstr_pna1') }}
 
),
 
prod_mstr_pna1 as (
 
    select
 
        product_id as product_id,
        product_pricing as product_pricing,
        product_margin as product_margin,
        prod_date as prod_date,
        category_code as category_code
 
    from source_raw
 
)
 
select * from prod_mstr_pna1