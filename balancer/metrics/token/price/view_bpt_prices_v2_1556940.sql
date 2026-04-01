-- part of a query repo
-- query name: view bpt prices v2
-- query link: https://dune.com/queries/1556940


WITH bpt_trades AS (
    SELECT
        block_time,
        bpt_address,
        bpt_amount_raw,
        bpt_amount_raw / POWER(10, COALESCE(erc20a.decimals, 18)) AS bpt_amount,
        token_amount_raw,
        token_amount_raw / POWER(10, erc20b.decimals) AS token_amount,
        p.price * token_amount_raw / POWER(10, erc20b.decimals) AS usd_amount
    FROM (
        SELECT
            t.evt_block_time AS block_time,
            CASE 
                WHEN t.tokenIn = SUBSTRING(t.poolId, 0, 42) THEN t.tokenIn
                ELSE t.tokenOut
            END AS bpt_address,
            CASE 
                WHEN t.tokenIn = SUBSTRING(t.poolId, 0, 42) THEN t.amountIn
                ELSE t.amountOut
            END AS bpt_amount_raw,
            CASE 
                WHEN t.tokenIn = SUBSTRING(t.poolId, 0, 42) THEN t.tokenOut
                ELSE t.tokenIn
            END AS token_address,
            CASE
                WHEN t.tokenIn = SUBSTRING(t.poolId, 0, 42) THEN t.amountOut
                ELSE t.amountIn
            END AS token_amount_raw
        FROM balancer_v2_ethereum.Vault_evt_Swap t
        WHERE t.tokenIn = SUBSTRING(t.poolId, 0, 42)
        OR t.tokenOut = SUBSTRING(t.poolId, 0, 42)
    ) dexs
    LEFT JOIN tokens.erc20 erc20a ON erc20a.contract_address = dexs.bpt_address
    AND erc20a.blockchain = "ethereum"
    JOIN tokens.erc20  erc20b ON erc20b.contract_address = dexs.token_address
    AND erc20b.blockchain = "ethereum"
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', dexs.block_time)
    AND p.contract_address = dexs.token_address AND p.blockchain = "ethereum"
),

bpt_estimated_prices AS (
    SELECT
        block_time,
        bpt_address,
        usd_amount / bpt_amount AS price
    FROM
        bpt_trades
)

SELECT DISTINCT bpt_address
FROM bpt_estimated_prices