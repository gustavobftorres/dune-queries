-- part of a query repo
-- query name: Balancer CoWSwap AMM Surplus Breakdown
-- query link: https://dune.com/queries/3973837


with t1 AS(
    SELECT 
        CASE WHEN token_1_transfer_usd > 0
            THEN COALESCE(token_1_transfer_usd, -token_2_transfer_usd) + COALESCE(token_1_balance_usd, token_2_balance_usd)*COALESCE(token_2_transfer_usd, -token_1_transfer_usd)/(COALESCE(token_2_balance_usd, token_1_balance_usd)-COALESCE(token_2_transfer_usd, -token_1_transfer_usd))
            ELSE COALESCE(token_2_transfer_usd, -token_1_transfer_usd) + COALESCE(token_2_balance_usd, token_1_balance_usd)*COALESCE(token_1_transfer_usd, -token_2_transfer_usd)/(COALESCE(token_1_balance_usd, token_2_balance_usd)-COALESCE(token_1_transfer_usd, -token_2_transfer_usd)) 
            END AS surplus, 
            COALESCE(protocol_fee_usd, 0) AS protocol_fee_usd, 
            d.tx_hash,
            CASE WHEN token_1_transfer_usd > 0 AND token_sold_address = token_1_address AND token_bought_address = token_2_address THEN 4
            WHEN token_2_transfer_usd > 0 AND token_sold_address = token_2_address AND token_bought_address = token_1_address THEN 4 ELSE 0 
            END AS trade_match
    FROM dune.balancer.result_b_cow_amm_base_table d
    JOIN balancer_cowswap_amm.trades c 
        ON d.tx_hash = c.tx_hash 
        AND c.blockchain = d.blockchain
    WHERE istrade 
    AND cow_amm_address = {{1. Pool Address}} 
    AND token_1_transfer*token_2_transfer < 0
    AND d.blockchain = '{{4. Blockchain}}'
    AND d.time >= TIMESTAMP '{{2. Start date}}'
    AND d.time <= TIMESTAMP '{{3. End date}}'
    
    UNION ALL 
    
    SELECT CASE WHEN token_1_transfer_usd > 0 
                THEN COALESCE(token_1_transfer_usd, -token_2_transfer_usd) + COALESCE(token_1_balance_usd, token_2_balance_usd) * COALESCE(token_2_transfer_usd, -token_1_transfer_usd) / (COALESCE(token_2_balance_usd, token_1_balance_usd) - COALESCE(token_2_transfer_usd, -token_1_transfer_usd))
                ELSE COALESCE(token_2_transfer_usd, -token_1_transfer_usd) + COALESCE(token_2_balance_usd, token_1_balance_usd) * COALESCE(token_1_transfer_usd, -token_2_transfer_usd) / (COALESCE(token_1_balance_usd, token_2_balance_usd) - COALESCE(token_1_transfer_usd, -token_2_transfer_usd)) 
                END AS surplus, 
            COALESCE(protocol_fee_usd, 0) AS protocol_fee_usd, d.tx_hash,
            CASE WHEN token_1_transfer_usd > 0 AND contract_address = token_1_address AND to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN 1
                 WHEN token_1_transfer_usd > 0 AND contract_address = token_2_address AND "FROM" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN -1
                 WHEN token_2_transfer_usd > 0 AND contract_address = token_2_address AND to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN 1
                 WHEN token_2_transfer_usd > 0 AND contract_address = token_1_address AND "FROM" = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 THEN -1
                 ELSE 0 END AS trade_match
    FROM dune.balancer.result_b_cow_amm_base_table d
    JOIN erc20_{{4. Blockchain}}.evt_transfer e 
        ON d.tx_hash = e.evt_tx_hash
    WHERE istrade 
    AND cow_amm_address = {{1. Pool Address}}
    AND token_1_transfer * token_2_transfer < 0 
    AND d.blockchain = '{{4. Blockchain}}'
    AND d.time >= TIMESTAMP '{{2. Start date}}'
    AND d.time <= TIMESTAMP '{{3. End date}}'
    ),
    
    t2 AS (
        SELECT 
            MAX(surplus) AS surplus, 
            MAX(protocol_fee_usd) AS protocol_fee_usd, 
            CASE WHEN MAX(trade_match) = 4 THEN 1 WHEN MAX(trade_match) - MIN(trade_match) = 2 THEN 0 ELSE 1 
            END AS trade_match
        FROM t1
        GROUP BY tx_hash)


SELECT 
    SUM(surplus) + SUM(protocol_fee_usd) AS total_surplus,
    SUM(surplus * least(1,trade_match)) + SUM(protocol_fee_usd*least(1,trade_match)) AS fees,
    SUM(surplus * (1-least(1,trade_match))) + SUM(protocol_fee_usd*(1-least(1,trade_match))) AS rebalancing_surplus
    FROM t2