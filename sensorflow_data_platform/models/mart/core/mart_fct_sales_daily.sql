select
    transaction_date,
    date_trunc('month', transaction_date)    as transaction_month,
    date_trunc('year',  transaction_date)    as transaction_year,
    item_code,
    item_name, 
    category_name,
    is_discounted,
    transaction_type,

    sum(quantity_sold_kg)             as total_qty_kg,
    sum(gross_revenue_rmb)            as total_revenue_rmb,
    avg(unit_price_rmb_per_kg)    as avg_selling_price,
    avg(wholesale_price_rmb_per_kg)       as avg_wholesale_price,
    avg(gross_margin_per_kg)            as avg_gross_margin_per_kg,
    avg(net_margin_per_kg)            as avg_net_margin_per_kg,
    sum(quantity_sold_kg * net_margin_per_kg)  as total_net_profit_rmb

from {{ ref('int_daily_margins') }}
group by all
