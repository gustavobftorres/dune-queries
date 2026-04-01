-- part of a query repo
-- query name: Ethereum query after compile
-- query link: https://dune.com/queries/6521384


WITH pool_labels AS (
        SELECT * FROM (
            SELECT
                address,
                name,
                pool_type,
                ROW_NUMBER() OVER (PARTITION BY address ORDER BY MAX(updated_at) DESC) AS num
            FROM "delta_prod"."labels"."balancer_v2_pools_ethereum"
            WHERE blockchain = 'ethereum'
            AND source = 'query'
            AND model_name = 'balancer_v2_pools_ethereum'
            GROUP BY 1, 2, 3) 
        WHERE num = 1
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM "delta_prod"."prices"."usd"
        WHERE blockchain = 'ethereum'
        GROUP BY 1, 2, 3

    ),

    bpt_prices_1 AS (
        SELECT 
            l.day,
            s.token_address AS token,
            18 AS decimals,
            SUM(protocol_liquidity_usd / supply) AS price
        FROM balancer.liquidity l
        LEFT JOIN balancer.bpt_supply s ON s.token_address = l.pool_address 
        AND l.blockchain = s.blockchain AND s.day = l.day AND s.supply > 1e-4
        WHERE l.blockchain = 'ethereum'
        AND l.version = '2'
        GROUP BY 1, 2, 3
    ),

    bpt_prices AS (
        SELECT  
            day,
            token,
            decimals,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change
        FROM bpt_prices_1
    ),

    daily_protocol_fee_collected AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            0xba12222222228d8ba445958a75a0704d566bf2c8 AS pool_id,
            token AS token_address,
            SUM(feeAmount) AS protocol_fee_amount_raw
        FROM "delta_prod"."balancer_v2_ethereum"."Vault_evt_FlashLoan" b
        GROUP BY 1, 2, 3 

        UNION ALL      

        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token AS token_address,
            SUM(protocol_fees) AS protocol_fee_amount_raw
        FROM "delta_prod"."balancer_v2_ethereum"."Vault_evt_PoolBalanceChanged" b
        CROSS JOIN unnest("protocolFeeAmounts", "tokens") AS t(protocol_fees, token)   
        GROUP BY 1, 2, 3 

        UNION ALL          

        SELECT
            date_trunc('day', t.evt_block_time) AS day,
            poolId AS pool_id,
            b.poolAddress AS token_address,
            sum(value) AS protocol_fee_amount_raw
        FROM "delta_prod"."balancer_v2_ethereum"."Vault_evt_PoolRegistered" b
        INNER JOIN "delta_prod"."erc20_ethereum"."evt_Transfer" t
            ON t.contract_address = b.poolAddress
            AND t."from" = 0x0000000000000000000000000000000000000000
            AND t."to" =
                CASE
                    WHEN 'ethereum' = 'fantom' THEN 0xc6920d3a369e7c8bd1a22dbe385e11d1f7af948f
                    ELSE 0xce88686553686DA562CE7Cea497CE749DA109f9F
                    END 
        GROUP BY 1, 2, 3
    ),

    decorated_protocol_fee AS (
        SELECT 
            d.day,
            d.pool_id,
            d.token_address,
            MAX(t.symbol) AS token_symbol,
            SUM(d.protocol_fee_amount_raw) AS token_amount_raw, 
            SUM(d.protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals))) AS token_amount,
            SUM(COALESCE(p1.price, p2.price, p3.price) * protocol_fee_amount_raw / POWER(10, COALESCE(t.decimals,p1.decimals, p3.decimals))) AS protocol_fee_collected_usd
        FROM daily_protocol_fee_collected d
        LEFT JOIN prices p1
            ON p1.token = d.token_address
            AND p1.day = d.day
        LEFT JOIN prices.day p2
            ON p2.contract_address = d.token_address
            AND p2.timestamp = d.day
        LEFT JOIN bpt_prices p3
            ON p3.token = d.token_address
            AND p3.day <= d.day
            AND d.day < p3.day_of_next_change     
        LEFT JOIN "delta_prod"."tokens"."erc20" t 
            ON t.contract_address = d.token_address
            AND t.blockchain = 'ethereum'
        GROUP BY 1, 2, 3
    ),

    revenue_share as(
        SELECT
        day,
        CASE 
            WHEN day < DATE '2022-07-03' THEN 0.25
            WHEN day >= DATE '2022-07-03' AND day < DATE '2023-01-23' THEN 0.25
            WHEN day >= DATE '2023-01-23' AND day < DATE '2023-07-24' THEN 0.35
            WHEN day >= DATE '2023-07-24' THEN 0.175
        END AS treasury_share
    FROM UNNEST(SEQUENCE(DATE '2022-03-01', CURRENT_DATE, INTERVAL '1' DAY)) AS date(day)
    )


    SELECT
        f.day,
        f.pool_id,
        BYTEARRAY_SUBSTRING(f.pool_id,1,20) AS pool_address,
        CASE WHEN f.pool_id = 0xba12222222228d8ba445958a75a0704d566bf2c8 THEN 'flashloan' ELSE l.name END AS pool_symbol,
        '2' AS version,
        'ethereum' AS blockchain,
        l.pool_type,
        CASE WHEN f.pool_id = 0xba12222222228d8ba445958a75a0704d566bf2c8 THEN 'flashloan' ELSE 'v2' END AS fee_type,
        f.token_address,
        f.token_symbol,
        SUM(f.token_amount_raw) AS token_amount_raw,
        SUM(f.token_amount) AS token_amount,
        SUM(f.protocol_fee_collected_usd) AS protocol_fee_collected_usd, 
        r.treasury_share,
        SUM(f.protocol_fee_collected_usd) * r.treasury_share AS treasury_fee_usd,
        SUM(f.protocol_fee_collected_usd) AS lp_fee_collected_usd
    FROM decorated_protocol_fee f
    INNER JOIN revenue_share r 
        ON r.day = f.day
    LEFT JOIN pool_labels l
        ON BYTEARRAY_SUBSTRING(f.pool_id,1,20) = l.address
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14

