-- part of a query repo
-- query name: protocol_fee_pool_snapshots
-- query link: https://dune.com/queries/4647533


WITH prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            blockchain,
            contract_address AS token,
            decimals,
            APPROX_PERCENTILE(price, 0.5) AS price
        FROM prices.usd
        WHERE blockchain IN ('arbitrum', 'base', 'ethereum', 'gnosis')
        AND minute > TIMESTAMP '2024-12-05 00:00'
        GROUP BY 1, 2, 3, 4
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            blockchain,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM dex.prices
        WHERE blockchain IN ('arbitrum', 'base', 'ethereum', 'gnosis')
        AND contract_address NOT IN (0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38, 0xde1e704dae0b4051e80dabb26ab6ad6c12262da0, 0x5ddb92a5340fd0ead3987d3661afcd6104c3b757) 
        AND hour > TIMESTAMP '2024-12-05 00:00'
        GROUP BY 1, 2, 3
        HAVING sum(sample_size) > 3
    ),

    dex_prices_2 AS(
        SELECT
            day,
            blockchain,
            token,
            price,
            lag(price) OVER(PARTITION BY token, blockchain ORDER BY day) AS previous_price
        FROM dex_prices_1
    ),

    dex_prices AS (
        SELECT
            day,
            blockchain,
            token,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, blockchain ORDER BY DAY) AS day_of_next_change
        FROM dex_prices_2
        WHERE (price < previous_price * 1e4 AND price > previous_price / 1e4)
    ),

    erc4626_prices AS (
        SELECT
            DATE_TRUNC('day', minute) AS day,
            blockchain,
            wrapped_token AS token,
            decimals,
            APPROX_PERCENTILE(median_price, 0.5) AS price,
            LEAD(DATE_TRUNC('day', minute), 1, CURRENT_DATE + INTERVAL '1' day) OVER (PARTITION BY wrapped_token ORDER BY date_trunc('day', minute)) AS next_change
        FROM balancer_v3.erc4626_token_prices
        WHERE blockchain IN ('arbitrum', 'base', 'ethereum', 'gnosis')
        GROUP BY 1, 2, 3, 4
    ),

snapshots AS(
SELECT 
    from_unixtime(day) AS day, 
    'gnosis' AS blockchain,
    pool, 
    token_address,
    token_symbol,
    CAST(total_protocol_swap_fee AS DOUBLE) AS total_protocol_swap_fee,
    CAST(total_protocol_yield_fee AS DOUBLE) AS total_protocol_yield_fee,
    CAST(protocol_swap_fee_vault_balance AS DOUBLE) AS protocol_swap_fee_vault_balance,
    CAST(protocol_yield_fee_vault_balance AS DOUBLE) AS protocol_yield_fee_vault_balance,
    CAST(protocol_fee_controller_balance AS DOUBLE) AS protocol_fee_controller_balance
FROM dune.balancer.dataset_v3_gnosis_snapshots

UNION 

SELECT 
    from_unixtime(day), 
    'ethereum' AS blockchain,
    pool, 
    token_address,
    token_symbol,
    CAST(total_protocol_swap_fee AS DOUBLE),
    CAST(total_protocol_yield_fee AS DOUBLE),
    CAST(protocol_swap_fee_vault_balance AS DOUBLE),
    CAST(protocol_yield_fee_vault_balance AS DOUBLE),
    CAST(protocol_fee_controller_balance AS DOUBLE)
FROM dune.balancer.dataset_v3_ethereum_snapshots

UNION 

SELECT 
    from_unixtime(day), 
    'arbitrum' AS blockchain,
    pool, 
    token_address,
    token_symbol,
    CAST(total_protocol_swap_fee AS DOUBLE),
    CAST(total_protocol_yield_fee AS DOUBLE),
    CAST(protocol_swap_fee_vault_balance AS DOUBLE),
    CAST(protocol_yield_fee_vault_balance AS DOUBLE),
    CAST(protocol_fee_controller_balance AS DOUBLE)
FROM dune.balancer.dataset_v3_arbitrum_snapshots

UNION 

SELECT 
    from_unixtime(day), 
    'base' AS blockchain,
    pool, 
    token_address,
    token_symbol,
    CAST(total_protocol_swap_fee AS DOUBLE),
    CAST(total_protocol_yield_fee AS DOUBLE),
    CAST(protocol_swap_fee_vault_balance AS DOUBLE),
    CAST(protocol_yield_fee_vault_balance AS DOUBLE),
    CAST(protocol_fee_controller_balance AS DOUBLE)
FROM dune.balancer.dataset_v3_base_snapshots)

SELECT
    s.day, 
    s.blockchain,
    s.pool, 
    l.pool_type,
    l.name AS pool_symbol,
    s.token_address,
    s.token_symbol,
    s.total_protocol_swap_fee,
    s.total_protocol_swap_fee * COALESCE(p1.price, p2.price, p3.price) AS total_protocol_swap_fee_usd,
    s.total_protocol_yield_fee,
    s.total_protocol_yield_fee * COALESCE(p1.price, p2.price, p3.price) AS total_protocol_yield_fee_usd,
    s.protocol_swap_fee_vault_balance,
    s.protocol_swap_fee_vault_balance * COALESCE(p1.price, p2.price, p3.price) AS protocol_swap_fee_vault_balance_usd,
    s.protocol_yield_fee_vault_balance,
    s.protocol_yield_fee_vault_balance * COALESCE(p1.price, p2.price, p3.price) AS protocol_yield_fee_vault_balance_usd,
    s.protocol_fee_controller_balance,
    s.protocol_fee_controller_balance * COALESCE(p1.price, p2.price, p3.price) AS protocol_fee_controller_balance_usd
FROM snapshots s
LEFT JOIN labels.balancer_v3_pools l ON s.blockchain = l.blockchain AND s.pool = l.address
LEFT JOIN prices p1 ON p1.day = s.day
AND p1.token = s.token_address
LEFT JOIN dex_prices p2 ON p2.day <= s.day
AND s.day < p2.day_of_next_change
AND p2.token = s.token_address
LEFT JOIN erc4626_prices p3 ON p3.day <= s.day
AND s.day < p3.next_change
AND p3.token = s.token_address
ORDER BY 1 DESC, 8 DESC