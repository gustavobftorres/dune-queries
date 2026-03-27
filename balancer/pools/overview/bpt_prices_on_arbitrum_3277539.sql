-- part of a query repo
-- query name: BPT Prices on Arbitrum
-- query link: https://dune.com/queries/3277539


WITH erc20_bpt_supply AS (
        SELECT
            block_day AS day,
            token_address AS token,
            SUM(amount_raw) / POW(10, 18) AS supply
        FROM balances_arbitrum.erc20_day e
        JOIN balancer_v2_arbitrum.Vault_evt_PoolRegistered b
        ON b.poolAddress = e.token_address
        AND e.token_address NOT IN (
            SELECT DISTINCT token_address
            FROM balancer_v2_arbitrum.bpt_supply
        )
        AND wallet_address != 0x0000000000000000000000000000000000000000
        GROUP BY 1, 2
    ),

    raw_bpt_supply AS (
        SELECT
            *,
            token_address AS token
        FROM balancer_v2_arbitrum.bpt_supply
    ),

    ranked_bpt_supply AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY day, token ORDER BY day DESC) AS row_number
        FROM raw_bpt_supply
    ),
    
    daily_bpt_supply AS (
        SELECT
            day,
            token,
            supply
        FROM ranked_bpt_supply
        WHERE row_number = 1
        
        UNION ALL
        
        SELECT
            day,
            token,
            supply
        FROM erc20_bpt_supply
        WHERE supply > 0
        AND supply < 2500000000000000 -- hack to remove pre-minted BPTs
    ),
    
    pools_liquidity AS (
        SELECT
            day,
            pool_address,
            SUM(pool_liquidity_usd) AS liquidity
        FROM balancer_v2_arbitrum.liquidity
        GROUP BY 1, 2
    )

SELECT
    p.day,
    p.pool_address AS token,
    liquidity / supply AS price
FROM pools_liquidity p
JOIN daily_bpt_supply d
ON d.token = p.pool_address
AND d.day = p.day
