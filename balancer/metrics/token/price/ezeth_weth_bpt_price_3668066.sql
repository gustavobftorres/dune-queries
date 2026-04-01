-- part of a query repo
-- query name: ezETH-WETH BPT price
-- query link: https://dune.com/queries/3668066


WITH bpt_trades AS (
        SELECT * 
        FROM balancer_v2_ethereum.Vault_evt_Swap v
        WHERE v.tokenIn = 0x596192bb6e41802428ac943d2f1476c1af25cc0e OR v.tokenOut = 0x596192bb6e41802428ac943d2f1476c1af25cc0e
    ), 

    all_trades_info AS (
        SELECT
            a.evt_tx_hash AS tx_hash,
            a.evt_block_time AS block_time,
            a.evt_block_number AS block_number,
            a.poolId AS pool_id,
            bytearray_substring(a.poolId, 1, 20) AS bpt_address,
            a.tokenIn AS token_in,
            CAST(a.amountIn AS DOUBLE) AS amount_in,
            a.tokenOut AS token_out,
            CAST(a.amountOut AS DOUBLE) AS amount_out,
            p1.price AS token_in_p,
            COALESCE(p1.symbol, t1.symbol) AS token_in_sym,
            COALESCE(p1.decimals, t1.decimals) AS token_in_decimals,
            p2.price AS token_out_p,
            COALESCE(p2.symbol, t2.symbol) AS token_out_sym,
            COALESCE(p2.decimals, t2.decimals) AS token_out_decimals
        FROM bpt_trades a
        LEFT JOIN prices.usd p1 ON p1.contract_address = a.tokenIn AND p1.blockchain = 'ethereum'
        AND  p1.minute = date_trunc('minute', a.evt_block_time)
        LEFT JOIN prices.usd p2 ON p2.contract_address = a.tokenOut AND p2.blockchain = 'ethereum'
        AND  p2.minute = date_trunc('minute', a.evt_block_time)
        LEFT JOIN tokens.erc20 t1 ON t1.contract_address = a.tokenIn AND t1.blockchain = 'ethereum'
        LEFT JOIN tokens.erc20 t2 ON t2.contract_address = a.tokenOut AND t2.blockchain = 'ethereum'
        ORDER BY a.evt_block_number DESC, a.evt_index DESC
    ),

    all_trades_calc_2 AS (
        SELECT *,
            amount_in / POWER(10, COALESCE(token_in_decimals, 18)) AS amount_in_norm,
            amount_out / POWER(10, COALESCE(token_out_decimals, 18)) AS amount_out_norm,
            (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) / (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) AS in_out_norm_rate,
            (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) / (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) AS out_in_norm_rate,
            CASE
                WHEN token_in_p IS NULL AND token_out_p IS NULL THEN NULL
                ELSE COALESCE(
                        token_in_p,
                        (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) / (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) * token_out_p
                    )
            END AS token_in_price,
            CASE
                WHEN token_in_p IS NULL AND token_out_p IS NULL THEN NULL
                ELSE COALESCE(
                        token_out_p,
                        (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) / (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) * token_in_p
                    )
            END AS token_out_price
        FROM all_trades_info
    ),

    unique_tx_token_price AS (
        SELECT
            distinct
                tx_hash,
                token,
                AVG(token_price) OVER(PARTITION BY tx_hash, token) AS avg_price
        FROM (
            SELECT tx_hash, token_in AS token, token_in_price AS token_price
            FROM all_trades_calc_2
            UNION ALL
            SELECT tx_hash, token_out AS token, token_out_price AS token_price
            FROM all_trades_calc_2
        )

        ORDER BY 1,2
    ),

    backfill_pricing_1 AS (
        SELECT
            c2.block_time,
            c2.tx_hash,
            c2.bpt_address,
            c2.token_in,
            c2.in_out_norm_rate,
            COALESCE(c2.token_in_price, u1.avg_price) AS token_in_price,
            c2.token_out,
            c2.out_in_norm_rate,
            COALESCE(c2.token_out_price, u2.avg_price) AS token_out_price
        FROM all_trades_calc_2 c2
        LEFT JOIN unique_tx_token_price u1 ON u1.tx_hash = c2.tx_hash AND u1.token = c2.token_in
        LEFT JOIN unique_tx_token_price u2 ON u2.tx_hash = c2.tx_hash AND u2.token = c2.token_out
    ),

    backfill_pricing_2 AS (
        SELECT
            block_time,
            tx_hash,
            bpt_address AS contract_address,
            token_in,
            COALESCE(token_in_price, (out_in_norm_rate * token_out_price)) AS token_in_price,
            token_out,
            COALESCE(token_out_price, (in_out_norm_rate * token_in_price)) AS token_out_price,
            in_out_norm_rate,
            out_in_norm_rate
        FROM backfill_pricing_1
    ),

    trade_price_formulation AS (
        SELECT
            block_time AS day,
            contract_address,
            approx_percentile(price, 0.5) FILTER (WHERE is_finite(price)) AS median_price
        FROM (
            SELECT block_time, contract_address, token_in_price AS price
            FROM backfill_pricing_2 b2 WHERE b2.contract_address = b2.token_in
            UNION
            SELECT block_time, contract_address, token_out_price AS price
            FROM backfill_pricing_2 b2 WHERE b2.contract_address = b2.token_out
        )
        GROUP BY 1, 2
    ),

    trade_price_formulation_2 AS(
        SELECT
            day,
            contract_address,
            CASE
                WHEN median_price IS NOT NULL THEN median_price
                WHEN LEAD(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC) IS NOT NULL
                THEN LEAD(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC)
                WHEN LAG(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC) IS NOT NULL
                THEN LAG(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC)
                ELSE approx_percentile(median_price, 0.5) OVER(
                        PARTITION BY contract_address ORDER BY day
                        ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
                    )
            END AS median_price
        FROM trade_price_formulation
    ),

    price_formulation AS(
        SELECT
            day,
            contract_address,
            median_price
        FROM (
            SELECT
                day,
                contract_address,
                median_price,
                AVG(median_price) OVER (PARTITION BY contract_address) AS avg_median_price
            FROM trade_price_formulation
        ) subquery
        WHERE median_price < avg_median_price * 2 --removes outliers
        AND date_trunc('day', day) = current_date
        GROUP BY 1, 2, 3
    )
SELECT * FROM price_formulation