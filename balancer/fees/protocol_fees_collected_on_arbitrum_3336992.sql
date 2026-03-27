-- part of a query repo
-- query name: Protocol Fees Collected on Arbitrum
-- query link: https://dune.com/queries/3336992


WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM labels.balancer_v2_pools
            WHERE blockchain = 'arbitrum'
            GROUP BY 1, 2) 
        WHERE num = 1
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM "delta_prod"."prices"."usd"
        WHERE blockchain = 'arbitrum'
                   
        GROUP BY 1, 2, 3

    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', hour) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM dex.prices
        
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),
 
    dex_prices AS (
        SELECT
            *,
            LEAD(DAY, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    DAY
            ) AS day_of_next_change
        FROM dex_prices_1
    ),

    bpt_prices AS(
        SELECT 
            day,
            token,
            price
        FROM dune.balancer.result_bpt_prices_on_arbitrum_2
    ),

    daily_protocol_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM "delta_prod"."balancer_v2_arbitrum"."Vault_evt_PoolBalanceChanged" b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)
                
        GROUP BY 1, 2, 3 

        UNION ALL          

        SELECT
            date_trunc('day', t.evt_block_time) AS day,
            poolId AS pool_id,
            b.poolAddress AS token_address,
            sum(value) AS protocol_fee_amount_raw
        FROM "delta_prod"."balancer_v2_arbitrum"."Vault_evt_PoolRegistered" b
        INNER JOIN "delta_prod"."erc20_arbitrum"."evt_transfer" t
            ON t.contract_address = b.poolAddress
            AND t."from" = 0x0000000000000000000000000000000000000000
            AND t.to = 0xce88686553686DA562CE7Cea497CE749DA109f9F --ProtocolFeesCollector address, which is the same across all chains
             
        GROUP BY 1, 2, 3
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day, 
            d.pool_id, 
            d.token_address, 
            t.symbol AS token_symbol,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw, 
            SUM(d.protocol_fee_amount_raw / power(10, COALESCE(t.decimals,p1.decimals))) AS token_amount,
            CASE 
                WHEN BYTEARRAY_SUBSTRING(d.pool_id, 1, 20) = d.token_address -- fees paid in BPTs
                    THEN SUM(p3.price * protocol_fee_amount_raw / POWER(10, 18))
                ELSE
                    SUM(COALESCE(p1.price, p2.price) * protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals))) 
            END AS protocol_fee_collected_usd
        FROM daily_protocol_fee_collected d
        LEFT JOIN prices p1
            ON p1.token = d.token_address
            AND p1.day = d.day
        LEFT JOIN dex_prices p2
            ON p2.token = d.token_address
            AND p2.day = d.day
        LEFT JOIN bpt_prices p3
            ON p3.token = d.token_address
            AND p3.day = d.day
        LEFT JOIN tokens.erc20 t 
            ON t.contract_address = d.token_address
            AND t.blockchain = 'arbitrum'
        GROUP BY 1, 2, 3, 4
    )

SELECT
    f.day,
    f.pool_id,
    BYTEARRAY_SUBSTRING(f.pool_id,1,20) as pool_address,
    l.name AS pool_symbol,
    '2' as version,
    'arbitrum' as blockchain,
    SUM(f.protocol_fee_collected_usd) / 0.5 as protocol_fee_collected_usd
FROM decorated_protocol_fee f
LEFT JOIN pool_labels l
ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
WHERE f.day > TIMESTAMP '2023-01-01' -- filter some data, we also know protocol fee has been 50% this year
AND f.protocol_fee_collected_usd > 0
GROUP BY 1, 2, 3, 4, 5, 6
HAVING is_finite(SUM(f.protocol_fee_collected_usd))
ORDER BY 1 DESC, 2
