-- fct_item_lifecycle.sql
with monthly_item as (
    select
        item_code,
        item_name,
        category_name,
        transaction_month,
        sum(total_revenue_rmb)      as monthly_revenue,
        row_number() over (
            partition by item_code
            order by transaction_month
        )                           as month_number
    from {{ ref('mart_fct_sales_daily') }}
    group by 1,2,3,4
),
with_growth as (
    select
        *,
        lag(monthly_revenue) over (
            partition by item_code order by transaction_month
        )                           as prev_month_revenue,
        first_value(transaction_month) over (
            partition by item_code order by transaction_month
        )                           as first_sale_month,
        max(transaction_month) over (
            partition by item_code
        )                           as last_sale_month
    from monthly_item
)
select
    item_code,
    item_name,
    category_name,
    first_sale_month,
    last_sale_month,
    avg(
        (monthly_revenue - prev_month_revenue)
        / nullif(prev_month_revenue, 0) * 100
    )                               as avg_monthly_growth_rate_pct,
    case
        when avg(
            (monthly_revenue - prev_month_revenue)
            / nullif(prev_month_revenue, 0)
        ) >  0.05 then 'Growing'
        when avg(
            (monthly_revenue - prev_month_revenue)
            / nullif(prev_month_revenue, 0)
        ) < -0.05 then 'Declining'
        else 'Stable'
    end                             as lifecycle_stage
from with_growth
where prev_month_revenue is not null
group by 1,2,3,4,5