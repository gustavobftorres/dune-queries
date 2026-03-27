-- part of a query repo
-- query name: Velodrome Slipstream TVL
-- query link: https://dune.com/queries/3630446


 WITH   
 erc20 AS ( 
    SELECT 
        contract_address, 
        symbol, 
        decimals
    FROM tokens.erc20 
    WHERE blockchain = 'optimism'
),

eth_prices AS (
    SELECT 
        DATE_TRUNC('day', minute) as day,
        AVG(price) as eth_price
    FROM prices.usd
    WHERE symbol = 'ETH'
    GROUP BY 1
),
 
 daily_price AS(
    SELECT 
        DATE_TRUNC('day', evt_block_time) AS day,
        token, 
        avg(price)/1e18 AS price 
    FROM velodrome_v2_optimism.PriceFetcher_evt_PriceFetched
    WHERE price != 0
    GROUP BY 1,2),
    
 cl_pools AS(
        SELECT 
            pool, 
            token0, 
            token1
        FROM velodrome_v2_optimism.CLFactory_evt_PoolCreated
    ),
 
    transfer_in AS(
        SELECT 
            DATE_TRUNC('day', evt_block_time) AS day,
            t0.pool, 
            t1.contract_address AS token, 
            SUM(t1.value) AS amount
        FROM cl_pools t0 
        INNER JOIN erc20_optimism.evt_Transfer t1 ON (t0.token0 = t1.contract_address OR t0.token1 = t1.contract_address) AND t1.to = t0.pool
        GROUP BY 1, 2, 3
    ),
    
    transfer_out AS(
        SELECT
            DATE_TRUNC('day', evt_block_time) AS day,
            t0.pool, 
            t1.contract_address AS token, 
            SUM(t1.value) AS amount
        FROM cl_pools t0 
        INNER JOIN erc20_optimism.evt_Transfer t1 ON (t0.token0 = t1.contract_address OR t0.token1 = t1.contract_address) AND t1."from" = t0.pool
        GROUP BY 1, 2, 3
    ),
    
    in_and_out AS (
        SELECT
            COALESCE(in_transfers.pool, out_transfers.pool) AS pool,
            COALESCE(in_transfers.token, out_transfers.token) AS token,
            COALESCE(in_transfers.day, out_transfers.day) AS day,
            COALESCE(in_transfers.amount, 0) AS amount_in,
            COALESCE(out_transfers.amount, 0) AS amount_out
        FROM transfer_in in_transfers
        FULL OUTER JOIN transfer_out out_transfers ON in_transfers.pool = out_transfers.pool AND in_transfers.token = out_transfers.token AND in_transfers.day = out_transfers.day
    ),
    
    decorated_balances AS (
        SELECT
            pool,
            token,
            day,
            SUM(amount_in) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_in,
            SUM(amount_out) OVER (PARTITION BY pool, token ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rolling_out
        FROM in_and_out
    )

    SELECT 
        t.day, 
        pool,
        SUM(balance/POWER(10, decimals) * price) AS liquidity_usd,
        SUM(balance/POWER(10, decimals) * price / eth_price) AS liquidity_eth
    FROM(
        SELECT 
                day,
                pool, 
                token, 
                rolling_in - rolling_out AS balance
        from decorated_balances
        ) t INNER JOIN daily_price p ON t.day = p.day AND p.token = t.token
            INNER JOIN erc20 e ON e.contract_address = t.token
            LEFT JOIN eth_prices c ON t.day = c.day
    GROUP BY 1,2