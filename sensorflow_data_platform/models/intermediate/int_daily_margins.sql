select
    e.*,
    w.wholesale_price_rmb_per_kg,

    -- margin per kg sold
    e.unit_price_rmb_per_kg - w.wholesale_price_rmb_per_kg  as gross_margin_per_kg,

    -- margin adjusted for loss (waste reduces effective yield)
    (e.unit_price_rmb_per_kg * (1 - e.loss_rate))
        - w.wholesale_price_rmb_per_kg                          as net_margin_per_kg

from {{ ref('int_sales_enriched') }} e
asof join {{ ref('base_dim_wholesale_prices') }} w 
    on e.item_code = w.item_code
    and e.transaction_date >= w.date
