WITH base AS (
  SELECT
    CAST(
      STRPTIME(
        "Date" || ' ' || 
        CASE 
          WHEN SPLIT_PART("Time", ':', 1)::INT >= 24 
            THEN LPAD((SPLIT_PART("Time", ':', 1)::INT - 24)::VARCHAR, 2, '0')
                 || ':' || SPLIT_PART("Time", ':', 2)
          ELSE "Time"
        END,
        '%m/%d/%Y %H:%M.%S'
      ) 
      + CASE 
          WHEN SPLIT_PART("Time", ':', 1)::INT >= 24 
            THEN INTERVAL 1 DAY
          ELSE INTERVAL 0 DAY
        END
    AS TIMESTAMP) AS transaction_ts,

    CAST("Item Code" AS STRING)                      AS item_code,
    CAST("Quantity Sold (kilo)" AS DECIMAL(10,3))    AS quantity_sold_kg,
    CAST("Unit Selling Price (RMB/kg)" AS DECIMAL(10,2)) 
                                                     AS unit_price_rmb_per_kg,
    LOWER("Sale or Return")                          AS transaction_type,

    CASE 
      WHEN LOWER("Discount (Yes/No)") = 'yes' THEN TRUE
      ELSE FALSE
    END                                              AS is_discounted

  FROM {{ ref('annex2') }}
)

SELECT
  -- full timestamp
  transaction_ts,

  -- split fields
  DATE(transaction_ts)                      AS transaction_date,
  CAST(transaction_ts AS TIME)              AS transaction_time,
  EXTRACT(HOUR FROM transaction_ts)         AS transaction_hour,

  -- other fields
  item_code,
  quantity_sold_kg,
  unit_price_rmb_per_kg,
  transaction_type,
  is_discounted
  
FROM base