-- part of a query repo
-- query name: Balancer CoWSwap AMM Liquidity
-- query link: https://dune.com/queries/3964979


SELECT
    SUM(protocol_liquidity_usd) / 1e6 AS tvl,
    SUM(protocol_liquidity_eth) AS tvl_eth
FROM balancer_cowswap_amm.liquidity
WHERE day = (SELECT MAX(day) FROM balancer_cowswap_amm.liquidity)
AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
