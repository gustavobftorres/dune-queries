-- part of a query repo
-- query name: Optimism Metrics
-- query link: https://dune.com/queries/3771267


WITH daily_tvl AS (
        SELECT
            blockchain,
            day,
            SUM(protocol_liquidity_usd) AS tvl
        FROM balancer.liquidity
        WHERE blockchain IN ('optimism')
        AND day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    daily_volume AS (
        SELECT
            blockchain,
            date_trunc('day', block_time) AS day,
            SUM(amount_usd) AS volume,
            SUM(amount_usd * swap_fee) AS swap_fee
        FROM balancer_v2_optimism.trades
        WHERE block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    daily_total_fee AS (
        SELECT
            blockchain,
            day,
            SUM(protocol_fee_collected_usd) * 2 AS total_fee --to include fees that go directly to LPs
        FROM balancer.protocol_fee
        WHERE blockchain IN ('optimism')
        AND day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    daily_transactions AS (
        SELECT
            'optimism' AS blockchain,
            date_trunc('day', block_time) AS day,
            COUNT(*) AS n_transactions
        FROM optimism.transactions
        WHERE to = 0xba12222222228d8ba445958a75a0704d566bf2c8 
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    avg_metrics AS (
        SELECT
            tvl.blockchain,
            AVG(tvl) AS avg_tvl,
            AVG(volume) AS avg_volume,
            AVG(swap_fee) AS avg_swap_fee,
            AVG(total_fee) AS avg_total_fee,
            AVG(n_transactions) AS avg_transactions
        FROM daily_tvl tvl
        LEFT JOIN daily_volume vol
        ON tvl.blockchain = vol.blockchain
        AND tvl.day = vol.day
        LEFT JOIN daily_transactions txn
        ON tvl.blockchain = txn.blockchain
        AND tvl.day = txn.day
        LEFT JOIN daily_total_fee fee
        ON tvl.blockchain = fee.blockchain
        AND tvl.day = fee.day
        GROUP BY 1
    ),
    
    unique_addresses AS (
        SELECT
            'optimism' AS blockchain,
            COUNT(DISTINCT "from") AS unique_addresses
        FROM optimism.transactions
        WHERE to = 0xba12222222228d8ba445958a75a0704d566bf2c8
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
    )
    
SELECT a.*, u.unique_addresses
FROM avg_metrics a
LEFT JOIN unique_addresses u
ON u.blockchain = a.blockchain
