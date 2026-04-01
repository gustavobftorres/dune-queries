-- part of a query repo
-- query name: Balancer V3 Boosted Pool Liquidity by Underlying Token
-- query link: https://dune.com/queries/4432340


SELECT
    day,
    underlying_token_symbol,
    SUM(protocol_liquidity_usd) AS tvl_usd,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer.liquidity s
INNER JOIN query_4419172 m ON s.pool_address = m.address
AND s.blockchain = m.blockchain
INNER JOIN balancer_v3.erc4626_token_mapping t ON s.token_address = t.erc4626_token
AND t.blockchain = s.blockchain
WHERE version = '3'
AND ('{{blockchain}}' = 'All' or s.blockchain = '{{blockchain}}')
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC