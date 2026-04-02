select
    i.item_code,
    i.item_name,
    i.category_code,
    i.category_name,
    l.loss_rate,
    case
        when loss_rate < 5  then 'Low'
        when loss_rate BETWEEN 5 AND 10   then 'Medium'
        else 'High'
    end                        as loss_tier
from {{ ref('base_dim_items') }} i
left join {{ ref('base_dim_item_loss_rate') }} l using (item_code)
