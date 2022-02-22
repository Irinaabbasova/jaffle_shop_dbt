{{
    config(
        materialized='incremental'
        
    )
}}

with orders_by_week as (
    select
    count(*) as cnt_orders,
    cast (date_trunc('week', order_date) as date) AS WEEK_KEY,
    status

from {{ source('src', 'source_orders') }}  

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where order_date > (select max(order_date) from {{ source('src', 'source_orders') }} )

{% endif %}

group by status, cast (date_trunc('week', order_date) as date)

),

final as (
select 
    *
from orders_by_week 

)

select * from final