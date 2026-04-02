select
    f.transaction_date,
    f.transaction_month,
    f.transaction_year,
    i.category_name,
    i.item_name,
    i.loss_tier,
    sum(total_qty_kg)                                    as total_volume_kg,
    sum(total_revenue_rmb)                               as total_revenue_rmb,
    sum(total_net_profit_rmb)                            as total_net_profit_rmb,
    avg(avg_net_margin_per_kg)                           as avg_net_margin_per_kg,
    round(
        sum(total_net_profit_rmb) / nullif(sum(total_revenue_rmb), 0) * 100
    , 1)                                                 as net_margin_pct

from {{ ref('mart_fct_sales_daily') }} f
left join {{ ref('mart_dim_items') }} i using (item_code) 
GROUP BY ALL