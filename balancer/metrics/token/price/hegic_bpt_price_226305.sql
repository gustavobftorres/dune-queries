-- part of a query repo
-- query name: HEGIC BPT Price
-- query link: https://dune.com/queries/226305


WITH bpt_transfers AS (
        SELECT * 
        FROM balancer_v1.view_transfers_bpt
        WHERE contract_address = '\x7cd40497c8284df1018673e6b29c3d97ef811685'
    ),
    
    bpt_joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, SUM(amt)/1e18 AS amount
        FROM bpt_transfers e
        WHERE src = '\x0000000000000000000000000000000000000000'
        GROUP BY 1
    ),
    
    bpt_exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, -SUM(amt)/1e18 AS amount
        FROM bpt_transfers e
        WHERE dst = '\x0000000000000000000000000000000000000000'
        GROUP BY 1
    ),
    
    daily_delta_bpt_by_pool AS (
        SELECT day, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM bpt_joins j 
        UNION ALL
        SELECT * 
        FROM bpt_exits e) foo
        GROUP BY 1
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT 
            day, 
            amount, 
            LEAD(day::timestamptz, 1, CURRENT_DATE::timestamptz) OVER (ORDER BY day) AS next_day,
            SUM(amount) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
    calendar AS (
       SELECT generate_series(MIN(day), CURRENT_DATE, '1 day'::interval) AS day
       FROM cumulative_bpt_by_pool
    ),
    
    running_cumulative_bpt_by_pool as (
        SELECT c.day, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
    ),
    
    daily_total_bpt AS (
        SELECT day, SUM(amount_bpt) AS total_bpt
        FROM running_cumulative_bpt_by_pool
        GROUP BY 1
    ),
    
    liquidity AS (
        SELECT day, SUM(usd_amount) AS tvl
        FROM balancer_v1.view_liquidity
        WHERE pool_id = '\x7cd40497c8284df1018673e6b29c3d97ef811685'
        GROUP BY 1
    )
    
SELECT l.day, tvl/total_bpt AS bpt_price
FROM liquidity l
INNER JOIN daily_total_bpt d
ON d.day = l.day