-- part of a query repo
-- query name: General Info
-- query link: https://dune.com/queries/2605258


-- SELECT 1 as rn, AVG(total_tvl) AS a FROM query_2605078
-- UNION
-- SELECT 2 as rn, market_cap AS a FROM query_2604998
-- UNION
-- SELECT 3 as rn, "Volume on Balancer" AS a FROM query_2557257 WHERE rn = 1
-- UNION
-- SELECT 4 as rn, "Volume on Balancer" AS a FROM query_2557257 WHERE rn = 2
-- ORDER BY rn ASC

--SELECT * FROM balancer.trades limit 10
-- SELECT 
--     '7 Day Volume' AS name,
--     date_trunc('day', block_time) AS time,
--     sum(amount_usd) AS vol 
-- FROM balancer.trades
-- WHERE block_date >= current_date - INTERVAL '7' DAY GROUP BY 1
SELECT --*,
    '7 Day Volume' AS name,
    blockchain,
    block_date,
    block_time,
    COALESCE(token_bought_symbol, 'Others') AS token_bought_symbol,
    COALESCE(token_sold_symbol, 'Others') AS token_sold_symbol,
    COALESCE(token_pair, 'Others') AS token_pair,
    token_bought_amount,
    token_sold_amount,
    amount_usd,
    token_bought_address,
    token_sold_address,
    project_contract_address,
    tx_from,
    tx_to,
    date_trunc('day', block_time) AS time,
    sum(amount_usd) OVER(PARTITION BY block_date, token_bought_symbol) AS vol_token_bought,
    sum(amount_usd) OVER(PARTITION BY block_date, token_sold_symbol) AS vol_token_sold,
    sum(amount_usd) OVER(PARTITION BY block_date, token_pair) AS vol_token_pair,
    sum(amount_usd) OVER(PARTITION BY block_date, project_contract_address) AS vol_pool
FROM  balancer.trades
WHERE block_date >= current_date - INTERVAL '7' DAY 
--GROUP BY 1