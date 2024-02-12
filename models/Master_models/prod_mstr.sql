with prod_mstr_pna1 as (
 
    select * from {{ ref('prod_mstr_pna1') }}
 
),
prod_mstr_tpna1 as (
 
    select * from {{ ref('prod_mstr_tpna1') }}
 
)
 
,final as (
   
        select
           
            p.product_id as prod_id,
            pt.product_name as prod_nm,
            p.product_pricing as prod_pricing,
            p.product_margin as prod_margin,
            p.prod_date as prod_date,
            case when p.category_code = 'CAT-A' then 'SNACKS'
            when p.category_code = 'CAT-B' then 'CEREALS'
            when p.category_code = 'CAT-C' then 'PRINGLES'
            else
            'INVALID'
            end as catg_cd
 
            from prod_mstr_pna1 as p
            left join prod_mstr_tpna1 as pt
            on p.product_id = pt.product_id
           
 
)
select * from final