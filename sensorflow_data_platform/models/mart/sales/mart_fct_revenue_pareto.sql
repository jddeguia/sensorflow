with item_revenue as (
    select
        transaction_date,
        item_code,
        item_name,
        category_name,
        sum(total_revenue_rmb)      as item_revenue
    from {{ ref('mart_fct_sales_daily') }}
    group by all
), 

ranked as (
    select
        *,
        sum(item_revenue) over ()                                    as total_revenue,
        sum(item_revenue) over (order by item_revenue desc
                                rows between unbounded preceding
                                and current row)                     as running_revenue,
        row_number() over (order by item_revenue desc)               as revenue_rank
    from item_revenue
)
select
    *,
    round(item_revenue / total_revenue * 100, 2)            as revenue_share_pct,
    round(running_revenue / total_revenue * 100, 2)         as cumulative_revenue_pct
from ranked
