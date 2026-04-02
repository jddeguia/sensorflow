with daily_item as (
    select
        item_code,
        item_name,
        category_name,
        transaction_date,
        avg(unit_price_rmb_per_kg)     as avg_price,
        sum(quantity_sold_kg)   as total_qty
    from {{ ref('int_sales_enriched') }}
    where transaction_type = 'sale'
    group by all
), 
with_lag as (
    select         *,
        lag(avg_price) over (partition by item_code order by transaction_date)   as prev_price,        
        lag(total_qty) over (partition by item_code order by transaction_date)   as prev_qty
    from daily_item
)
select
    item_code,
    item_name,
    category_name,
    avg(
        case
            when prev_price is not null
             and prev_price != 0
             and prev_qty   != 0
            then
                ((total_qty - prev_qty) / prev_qty)
                / ((avg_price - prev_price) / prev_price)
        end
    )   as price_elasticity_coeff
from with_lag
group by all


---Elasticity < -1 means price-sensitive — don't raise price here. Between -1 and 0 — you have room to
---push price up.

