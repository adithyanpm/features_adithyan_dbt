with cust_mstr_kna1 as (
 
    select * from {{ ref('cust_mstr_kna1') }}
 
),
cust_mstr_tkna1 as (
 
    select * from {{ ref('cust_mstr_tkna1') }}
 
)
,
country as (
    select * from {{ref('country_code')}}
)
,final as (
   
        select
            c.cust_number as cust_nbr,
            concat(ct.first_name || ct.last_name) as cust_nm,
            c.cust_location as cust_loc,
            c.cust_country as cust_ctry_nm,
            cc.country_code as ctry_cd
 
            from cust_mstr_kna1 as c
            left join cust_mstr_tkna1 as ct
            on c.cust_number = ct.cust_number
            left join country as cc
            on c.cust_country = cc.country_name
 
)
select * from final