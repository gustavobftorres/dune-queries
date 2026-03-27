-- part of a query repo
-- query name: Optimism LST/LRT Liquidity Variation, by token
-- query link: https://dune.com/queries/3939213


WITH initial_tvl AS (
    SELECT 
        l.symbol, 
        SUM(protocol_liquidity_eth) AS initial_liquidity_eth,
        SUM(protocol_liquidity_usd) AS initial_liquidity_usd
    FROM balancer_v2_optimism.liquidity t
    INNER JOIN dune.balancer.result_lst_tokens l 
    ON t.token_address = l.contract_address
    AND l.blockchain = t.blockchain
    WHERE t.day = TIMESTAMP '{{Start date}}'
    AND t.blockchain = 'optimism'
    GROUP BY 1
    ORDER BY 1 DESC, 2 DESC
)

SELECT 
    l.symbol, 
    i.initial_liquidity_eth,
    i.initial_liquidity_usd,
    SUM(protocol_liquidity_eth) AS current_liquidity_eth,
    SUM(protocol_liquidity_usd) AS current_liquidity_usd,
    (SUM(protocol_liquidity_eth / i.initial_liquidity_eth) - 1)  AS liquidity_growth_eth,
    (SUM(protocol_liquidity_usd / i.initial_liquidity_usd) - 1)  AS liquidity_growth_usd
FROM balancer_v2_optimism.liquidity t
INNER JOIN dune.balancer.result_lst_tokens l 
ON t.token_address = l.contract_address
AND l.blockchain = t.blockchain
LEFT JOIN initial_tvl i 
ON i.symbol = l.symbol
WHERE t.day = (SELECT MAX(day) FROM balancer_v2_optimism.liquidity)
AND t.blockchain = 'optimism'
GROUP BY 1, 2, 3 
ORDER BY 1 DESC, 3 DESC
