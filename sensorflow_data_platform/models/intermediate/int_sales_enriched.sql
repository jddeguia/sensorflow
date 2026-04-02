select
    s.transaction_date,
    s.transaction_hour,
    s.transaction_time,
    s.item_code,
    i.item_name,
    i.category_name,
    s.transaction_type,
    s.is_discounted,
    s.quantity_sold_kg,
    s.unit_price_rmb_per_kg, 
    l.loss_rate,

    -- effective quantity after typical loss
    s.quantity_sold_kg * (1 - l.loss_rate)   as effective_qty_kg,

    -- gross revenue
    s.quantity_sold_kg * s.unit_price_rmb_per_kg  as gross_revenue_rmb

from {{ ref('base_fct_sales') }} s
left join {{ ref('base_dim_items') }} i        using (item_code)
left join {{ ref('base_dim_item_loss_rate') }} l   using (item_code)
