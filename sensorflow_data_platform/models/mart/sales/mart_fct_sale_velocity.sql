select 
    transaction_month,
    transaction_year,
    item_code,
    item_name,
    category_name,
    count(distinct transaction_date)                               as active_selling_days,
    sum(total_qty_kg)                                       as total_volume_kg,
    sum(total_qty_kg) / nullif(count(distinct transaction_date), 0) as avg_daily_velocity_kg,      
    stddev(total_qty_kg)                                    as volume_volatility,
    -- coefficient of variation: how unpredictable is daily demand?
    round(
        stddev(total_qty_kg)
        / nullif(avg(total_qty_kg), 0) 
    , 1)                                                    as demand_cv_pct
from {{ ref('mart_fct_sales_daily') }}
where transaction_type = 'sale'
group by all
