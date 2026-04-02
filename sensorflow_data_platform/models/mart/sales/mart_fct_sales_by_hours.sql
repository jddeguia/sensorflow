select
    transaction_date,
    date_trunc('month', transaction_date)          as sale_month,
    hour(transaction_time)                         as sale_hour,
    case
        when hour(transaction_time) between 6  and 11 then 'Morning'
        when hour(transaction_time) between 12 and 17 then 'Afternoon'
        when hour(transaction_time) between 18 and 21 then 'Evening'
        else 'Off-peak'
    end                                     as day_part,
    category_name,
    transaction_type,
    count(*)                                as transaction_count,
    sum(quantity_sold_kg)                   as total_qty_kg,
    sum(gross_revenue_rmb)                  as total_revenue_rmb, 
        avg(unit_price_rmb_per_kg)                     as avg_selling_price
from {{ ref('int_sales_enriched') }}
group by all
