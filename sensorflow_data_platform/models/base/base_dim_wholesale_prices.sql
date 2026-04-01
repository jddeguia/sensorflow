SELECT
  CAST(STRPTIME("Date", '%m/%d/%Y') AS DATE)    AS date,
  CAST("Item Code" AS BIGINT)                   AS item_code,
  CAST("Wholesale Price (RMB/kg)" AS DOUBLE)    AS wholesale_price_rmb_per_kg
FROM {{ ref('annex3') }}