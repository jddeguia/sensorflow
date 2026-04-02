select
    transaction_date,
    item_code, 
    item_name,
    category_name,
    sum(case when transaction_type = 'sale'   then quantity_sold_kg end)  as sold_kg,
    sum(case when transaction_type = 'return' then quantity_sold_kg end)  as returned_kg,
    round(
        sum(case when transaction_type = 'return' then quantity_sold_kg else 0 end)
        / nullif(sum(quantity_sold_kg), 0) * 100
    , 2)                                                                   as return_rate_pct,
    avg(case when transaction_type = 'return' then unit_price_rmb_per_kg end)    as avg_return_price
from {{ ref('int_sales_enriched') }}
group by all
having sold_kg > 0
