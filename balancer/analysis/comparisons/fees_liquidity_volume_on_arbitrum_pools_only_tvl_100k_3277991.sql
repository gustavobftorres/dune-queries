-- part of a query repo
-- query name: Fees, Liquidity, Volume on Arbitrum Pools [only TVL > 100k]
-- query link: https://dune.com/queries/3277991


WITH liquidity AS (
        SELECT
            day,
            pool_id,
            SUM(pool_liquidity_usd) AS pool_liquidity_usd
        FROM balancer.liquidity
        WHERE day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    volume AS (
        SELECT
            date_trunc('day', block_time) AS day,
            pool_id,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE date_trunc('day', block_time) <= TIMESTAMP '{{End date}}'
        AND date_trunc('day', block_time) >= TIMESTAMP '{{Start date}}'
        GROUP BY 1, 2
    ),
    
    total_fee AS (
        SELECT
            day,
            pool_id,
            pool_symbol,
            total_fee_usd
        FROM query_3274285 q
        WHERE day <= TIMESTAMP '{{End date}}'
        AND day >= TIMESTAMP '{{Start date}}'
    ),

    metrics AS (
        SELECT
            t.pool_id,
            t.pool_symbol as symbol,
            AVG(pool_liquidity_usd) AS "Avg. Liquidity",
            SUM(total_fee_usd) AS "Total Fee",
            SUM(volume) AS "Total Volume"
        FROM liquidity l
        JOIN total_fee  t
        ON t.pool_id = l.pool_id
        AND t.day = l.day
        JOIN volume  v
        ON v.pool_id = l.pool_id
        AND v.day = l.day
        GROUP BY 1, 2
        ORDER BY 3 DESC NULLS LAST
    )
    
SELECT *
FROM metrics
WHERE "Avg. Liquidity" > 100000
