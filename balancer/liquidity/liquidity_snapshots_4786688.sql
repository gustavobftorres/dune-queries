-- part of a query repo
-- query name: Liquidity snapshots
-- query link: https://dune.com/queries/4786688


WITH token_data AS (
        SELECT
            chain,
            pool,
            token_index,
            FROM_HEX(json_extract_scalar(token, '$.token')) AS token
        FROM (
            SELECT
                chain,
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM balancer_v3_multichain.vault_evt_poolregistered
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
    ),

pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM labels.balancer_v3_pools
    ),

    cumulative_balance AS (
        SELECT
            q.day,
            q.blockchain,
            t.pool AS pool_id,
            t.token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY t.token, t.pool , q.blockchain ORDER BY q.DAY) AS day_of_next_change,
            SUM(q.token_balance) AS cumulative_amount
        FROM query_4786655 q
        JOIN token_data t ON t.chain = q.blockchain
        AND q.pool_id = CAST(t.pool AS VARCHAR)
        AND q.token_index = t.token_index
        GROUP BY 1, 2, 3, 4
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2024-12-01'), date(now()), interval '1' day)) as t(date_sequence)
    ),

   cumulative_usd_balance AS (
        SELECT
            c.day,
            b.blockchain,
            b.pool_id,
            b.token,
            t.symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount AS token_balance,
            cumulative_amount * COALESCE(p1.price, 0) AS protocol_liquidity_usd,
            cumulative_amount * COALESCE(p1.price, 0) AS pool_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN tokens.erc20 t ON t.contract_address = b.token
        AND t.blockchain = b.blockchain
        LEFT JOIN prices.day p1 ON p1.timestamp = b.day
        AND p1.contract_address = b.token
        AND p1.blockchain = b.blockchain
        -- LEFT JOIN dex_prices p2 ON p2.day <= c.day
        -- AND c.day < p2.day_of_next_change
        -- AND p2.token = b.token
        -- AND p2.blockchain = b.blockchain
        -- LEFT JOIN bpt_prices p3 ON p3.day = b.day
        -- AND p3.token = b.token
        -- AND p3.blockchain = b.blockchain
        -- LEFT JOIN erc4626_prices p4 ON p4.day <= c.day
        -- AND c.day < p4.next_change
        -- AND p4.token = b.token
        -- AND p4.blockchain = b.blockchain
        WHERE b.token != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (partition by b.day, b.pool_id ORDER BY SUM(b.pool_liquidity_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.protocol_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS protocol_liquidity,
            SUM(b.pool_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1)  AS pool_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN balancer.pools_tokens_weights w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.pool_liquidity_usd > 0
        LEFT JOIN balancer.token_whitelist q ON b.token = q.address
        AND b.blockchain = q.chain
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        WHERE q.name IS NOT NULL
        AND p.pool_type IN ('weighted') -- filters for weighted pools with pricing assets
        AND w.blockchain = b.blockchain
        AND w.version = '3'
        GROUP BY 1, 2, 3, 4
    ),

    weighted_pool_liquidity_estimates_2 AS(
    SELECT  e.day,
            e.pool_id,
            SUM(e.pool_liquidity) / MAX(e.pricing_count) AS pool_liquidity,
            SUM(e.protocol_liquidity) / MAX(e.pricing_count) AS protocol_liquidity
    FROM weighted_pool_liquidity_estimates e
    GROUP BY 1,2
    )

    SELECT
        c.day,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        p.pool_symbol,
        '3' AS version,
        c.blockchain,
        p.pool_type,
        c.token AS token_address,
        c.token_symbol,
        c.token_balance_raw,
        c.token_balance,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd) AS protocol_liquidity_usd,
        -- COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd)/e.eth_price AS protocol_liquidity_eth,
        COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd) AS pool_liquidity_usd
        -- COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd)/e.eth_price AS pool_liquidity_eth
    FROM cumulative_usd_balance c
    FULL OUTER JOIN weighted_pool_liquidity_estimates_2 b ON c.day = b.day
    AND c.pool_id = b.pool_id
    LEFT JOIN balancer.pools_tokens_weights w ON b.pool_id = w.pool_id
    AND w.blockchain = c.blockchain
    AND w.version = '3'
    AND w.token_address = c.token
    -- LEFT JOIN eth_prices e ON e.day = c.day
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
