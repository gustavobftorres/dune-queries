-- part of a query repo
-- query name: Balancer Metrics on Arbitrum
-- query link: https://dune.com/queries/3222705


WITH daily_tvl AS (
        SELECT
            blockchain,
            day,
            CASE WHEN '{{General TVL Currency}}' = 'USD'
            THEN SUM(protocol_liquidity_usd) 
            WHEN '{{General TVL Currency}}' = 'ETH'
            THEN SUM(protocol_liquidity_eth) 
            END AS tvl
        FROM balancer.liquidity
        WHERE blockchain = 'arbitrum'
        AND day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        AND day != timestamp '2021-10-27' -- TODO: figure out this outlier
        GROUP BY 1, 2
    ),
    
    daily_volume AS (
        SELECT
            blockchain,
            date_trunc('day', block_time) AS day,
            SUM(amount_usd) AS volume,
            SUM(amount_usd * swap_fee) AS swap_fee
        FROM balancer_v2_arbitrum.trades
        WHERE blockchain = 'arbitrum'
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    daily_total_fee AS (
        SELECT
            'arbitrum' AS blockchain,
            day,
            SUM(protocol_fee_collected_usd) * 2 AS total_fee
        FROM balancer_v2_arbitrum.protocol_fee
        WHERE day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    daily_transactions AS (
        SELECT
            'arbitrum' AS blockchain,
            date_trunc('day', block_time) AS day,
            COUNT(*) AS n_transactions
        FROM arbitrum.transactions
        WHERE to = 0xBA12222222228d8Ba445958a75a0704d566BF2C8
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
            'arbitrum' AS blockchain,
            COUNT(DISTINCT "from") AS unique_addresses
        FROM arbitrum.transactions
        WHERE to = 0xBA12222222228d8Ba445958a75a0704d566BF2C8
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
    )
    
SELECT a.*, u.unique_addresses
FROM avg_metrics a
LEFT JOIN unique_addresses u
ON u.blockchain = a.blockchain
