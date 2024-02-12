--The total margin should be more than zero
--Otherwise it will be negative
 
select *
  from {{ ref('day_invoice') }}  
 where total_margin < 0