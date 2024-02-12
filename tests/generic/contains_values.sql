{% test contains_values(model, column_name) %}
 
with validation as (
 
    select
        {{ column_name }} as contains_field
 
    from {{ model }}
 
),
 
validation_errors as (
 
    select
        contains_field
 
    from validation
    -- if this is true, then Zone does not exists in A,B,C,D
    where contains_field not in ('A','B','C','D')
 
)
 
select *
from validation_errors
 
{% endtest %}