-- part of a query repo
-- query name: LST Comparison
-- query link: https://dune.com/queries/3634086


WITH swaps AS(
SELECT 
    t.project,
    l.symbol,
    SUM(CASE WHEN block_time >= now() - interval '30' day THEN amount_usd ELSE 0 END) AS volume_30d,
    SUM(CASE WHEN block_time >= now() - interval '7' day THEN amount_usd ELSE 0 END) AS volume_7d,
    SUM(CASE WHEN block_time >= now() - interval '24' hour THEN amount_usd ELSE 0 END) AS volume_1d
FROM balancer.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE project IN ('balancer')
AND t.blockchain IN ('optimism', 'base')
GROUP BY 1, 2

UNION ALL

SELECT 
    t.project,
    l.symbol,
    SUM(CASE WHEN block_time >= now() - interval '30' day THEN amount_usd ELSE 0 END) AS volume_30d,
    SUM(CASE WHEN block_time >= now() - interval '7' day THEN amount_usd ELSE 0 END) AS volume_7d,
    SUM(CASE WHEN block_time >= now() - interval '24' hour THEN amount_usd ELSE 0 END) AS volume_1d
FROM dex.trades t
INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
WHERE t.project IN ('aerodrome', 'velodrome')
GROUP BY 1, 2
),

liquidity AS(
SELECT 
    day,
    'balancer' AS project,
    token_symbol,
    SUM(protocol_liquidity_usd) AS liquidity
    FROM balancer.liquidity t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = t.blockchain AND l.contract_address = t.token_address
    WHERE day = (SELECT MAX(day) FROM balancer.liquidity)
    AND (t.blockchain IN ('optimism', 'base'))
    GROUP BY 1, 2, 3

UNION ALL

SELECT
    day,
    'aerodrome' AS project,
    l.symbol,
    SUM(liquidity_usd) AS liquidity
    FROM query_3625769 t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = 'base' AND l.contract_address = t.token
    WHERE day = (SELECT MAX(day) FROM balancer.liquidity)
    GROUP BY 1, 2, 3
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    l.symbol,
    SUM(liquidity_usd) AS liquidity
    FROM query_3625994 t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = 'optimism' AND l.contract_address = t.token
    WHERE day = (SELECT MAX(day) FROM balancer.liquidity)
    GROUP BY 1, 2, 3
)

SELECT 
    s.*,
    l.liquidity,
    s.volume_1d/l.liquidity AS capital_efficiency
FROM swaps s
LEFT JOIN liquidity l ON s.project = l.project AND s.symbol = l.token_symbol
WHERE ('{{LST}}' = 'All' OR s.symbol = '{{LST}}')
ORDER BY 3 DESC