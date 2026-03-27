-- part of a query repo
-- query name: Balancer Investments/Withdrawals
-- query link: https://dune.com/queries/232903


WITH prices_usd AS (
        SELECT
            date_trunc('day', MINUTE) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM prices.usd
        WHERE minute >= '2021-05-01'
        GROUP BY 1, 2
    ),
    
    balances_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            "poolId" AS pool_id,
            UNNEST(tokens) AS token,
            UNNEST(deltas) AS delta
        FROM balancer_v2."Vault_evt_PoolBalanceChanged"
        WHERE "poolId" = CONCAT('\', SUBSTRING('{{1. Pool ID}}', 2))::bytea
    ),
    
    daily_balances AS (
        SELECT 
            b.day, 
            pool_id, 
            b.token, 
            CASE 
                WHEN delta > 0 THEN 'Investments' 
                ELSE 'Withdrawals' 
            END AS kind,
            SUM(delta) AS amount 
        FROM balances_changes b
        GROUP BY 1, 2, 3, 4
    ),
    
    usd_daily_balances AS (
        SELECT b.day, pool_id, b.token, kind, (amount / 10 ^ decimals) * price AS usd_amount
        FROM daily_balances b
        LEFT JOIN erc20.tokens t
        ON t.contract_address = b.token
        LEFT JOIN prices_usd p
        ON p.token = b.token
        AND p.day = b.day
    )
    
SELECT 
    date_trunc('week', day) AS week,
    kind,
    SUM(usd_amount) AS usd_amount
FROM usd_daily_balances b
GROUP BY 1, 2
