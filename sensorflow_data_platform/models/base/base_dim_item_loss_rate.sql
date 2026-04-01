SELECT
  CAST("Item Code" AS STRING) AS item_code,
  CAST("Item Name" AS STRING) AS item_name,
  CAST("Loss Rate (%)" AS DECIMAL(5,2)) / 100 AS loss_rate
FROM {{ ref('annex4') }}