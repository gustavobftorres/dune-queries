-- part of a query repo
-- query name: Balancer Pools Investments/Withdrawals
-- query link: https://dune.com/queries/237505


WITH lm_pools AS (
        SELECT pool_id, SUM(amount)::int bal_mined
        FROM dune_user_generated.balancer_liquidity_mining
        WHERE day >= '{{2. Start Date}}'
        AND day <= '{{3. End Date}}'
        AND chain_id = '1'
        GROUP BY 1
    ),
    
    labels AS (
        SELECT * FROM (SELECT
            address,
            name,
            ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
        FROM labels.labels
        WHERE "type" = 'balancer_v2_pool'
        GROUP BY 1, 2) l
        WHERE num = 1
    ),
    
    prices_usd AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '{{2. Start Date}}'
        AND minute <= '{{3. End Date}}'
        GROUP BY 1, 2
    ),
    
    balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool_id,
            UNNEST(tokens) AS token,
            UNNEST(deltas) AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceChanged" b
        INNER JOIN lm_pools p ON p.pool_id = b."poolId"
        AND evt_block_time >= '{{2. Start Date}}'
        AND evt_block_time <= '{{3. End Date}}'
    ),
    
    daily_balances AS (
        SELECT 
            b.day, 
            pool_id, 
            b.token, 
            CASE WHEN delta > 0 THEN delta ELSE 0 
            END AS investments,
            CASE WHEN delta < 0 THEN delta ELSE 0
            END AS withdrawals
        FROM balances_changes b
    ),
    
    usd_daily_balances AS (
        SELECT 
            b.day,
            pool_id,
            b.token,
            SUM(price * investments / 10 ^ decimals) AS usd_investments,
            SUM(price * withdrawals / 10 ^ decimals) AS usd_withdrawals
        FROM daily_balances b
        LEFT JOIN erc20.tokens t
        ON t.contract_address = b.token
        LEFT JOIN prices_usd p
        ON p.token = b.token
        AND p.day = b.day
        GROUP BY 1, 2, 3
    )

SELECT
    CONCAT(SUBSTRING(UPPER(l.name), 0, 16)) AS composition,
    SUM(usd_investments) AS usd_investments,
    SUM(usd_withdrawals) AS usd_withdrawals
FROM usd_daily_balances b
LEFT JOIN labels l
ON l.address = SUBSTRING(b.pool_id, 0, 21)
GROUP BY 1
ORDER BY 2 DESC