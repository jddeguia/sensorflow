select
    CAST("Item Code" AS STRING) AS item_code,
    CAST("Item Name" AS STRING) AS item_name,
    CAST("Category Code" AS STRING) AS category_code,
    CAST("Category Name" AS STRING) AS category_name,
from {{ ref('annex1') }}
