with source_raw as (
 
    select * from {{ source('source_raw', 'invoice_raw') }}
 
),
 
invoice_raw as (
 
    select
 
        customer_nbr as customer_nbr,
        product_nbr as product_id,
        transaction_timestamp as transaction_timestamp,
        creation_date as creation_date,
        region as region,
        zone as zone,
        quantity as quantity,
        {{ dbt_utils.generate_surrogate_key(['customer_nbr', 'product_id', 'transaction_timestamp']) }} as hashkey
 
    from source_raw
 
)
 
select * from invoice_raw