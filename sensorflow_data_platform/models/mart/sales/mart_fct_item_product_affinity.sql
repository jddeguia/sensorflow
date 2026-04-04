with time_buckets as (
    select
        transaction_date,
        item_code,
        item_name,
        category_name,
        -- combine date + time into a timestamp first, then truncate
        date_trunc(
            'hour',
            CAST(transaction_date || ' ' || transaction_time AS TIMESTAMP)
        ) as time_bucket
    from {{ ref('int_sales_enriched') }}
    where transaction_type = 'sale'
),

pairs as (
    select
        a.item_code     as item_a,
        a.item_name     as item_name_a,
        b.item_code     as item_b,
        b.item_name     as item_name_b,
        count(*)        as co_occurrence_count
    from time_buckets a
    join time_buckets b
        on  a.transaction_date   = b.transaction_date
        and a.time_bucket = b.time_bucket
        and a.item_code   < b.item_code   -- avoid duplicates
    group by 1,2,3,4
)
select
    *,
    row_number() over (
        partition by item_a
        order by co_occurrence_count desc
    )                   as affinity_rank
from pairs
qualify affinity_rank <= 5  -- top 5 pairs per item
order by co_occurrence_count desc