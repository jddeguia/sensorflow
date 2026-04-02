select
    category_name,
    item_name,
    is_discounted,
    transaction_type,
    count(*)                                            as transaction_count,
    sum(total_qty_kg)                               as total_qty_kg,
    sum(total_revenue_rmb)                              as total_revenue_rmb,
    sum(total_net_profit_rmb)                           as total_net_profit_rmb,
    avg(avg_net_margin_per_kg)                          as avg_net_margin_per_kg,
    round(
        sum(total_net_profit_rmb)
        / nullif(sum(total_revenue_rmb), 0) * 100
    , 1)                                                as net_margin_pct,
    -- volume uplift proxy: discounted avg qty vs non-discounted
    avg(total_qty_kg)                                   as avg_basket_qty_kg
from {{ ref('mart_fct_sales_daily') }}
group by all
