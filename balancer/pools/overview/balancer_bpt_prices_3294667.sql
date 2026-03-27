-- part of a query repo
-- query name: Balancer BPT Prices
-- query link: https://dune.com/queries/3294667


WITH raw_bpt_supply AS (
        SELECT
            *,
            DATE_TRUNC('day', block_time) AS day,
            BYTEARRAY_SUBSTRING(pool_id, 1, 20) AS token
        FROM balancer.bpt_supply
    ),

    ranked_bpt_supply AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY day, blockchain, token ORDER BY block_time DESC) AS row_number
        FROM raw_bpt_supply
    ),
    
    daily_bpt_supply AS (
        SELECT
            day,
            token,
            blockchain,
            lp_virtual_supply AS supply
        FROM ranked_bpt_supply
        WHERE row_number = 1
    ),
    
    pools_liquidity AS (
        SELECT
            day,
            blockchain,
            pool_address,
            SUM(pool_liquidity_usd) AS liquidity
        FROM balancer.liquidity
        GROUP BY 1, 2, 3
    )

SELECT
    p.day,
    p.blockchain,
    18 as decimals,
    p.pool_address AS token,
    liquidity / supply AS price
FROM pools_liquidity p
JOIN daily_bpt_supply d
ON d.token = p.pool_address
AND d.blockchain = p.blockchain
AND d.day = p.day
