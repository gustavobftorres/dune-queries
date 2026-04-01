-- part of a query repo
-- query name: LST TVL comparison
-- query link: https://dune.com/queries/3629681


SELECT 
    day,
    'balancer' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(protocol_liquidity_eth)
    ELSE SUM(protocol_liquidity_usd)
    END AS liquidity
    FROM balancer.liquidity t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = t.blockchain AND l.contract_address = t.token_address
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR t.blockchain = '{{Balancer Blockchain}}')
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'aerodrome' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(liquidity_eth)
    ELSE SUM(liquidity_usd)
    END AS liquidity
    FROM query_3625769 t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = 'base' AND l.contract_address = t.token
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(liquidity_eth)
    ELSE SUM(liquidity_usd)
    END AS liquidity
    FROM query_3625994 t
    INNER JOIN dune.balancer.result_lst_tokens l ON l.blockchain = 'optimism' AND l.contract_address = t.token
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2