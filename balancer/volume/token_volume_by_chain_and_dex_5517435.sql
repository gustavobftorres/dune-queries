-- part of a query repo
-- query name: Token Volume by Chain and DEX
-- query link: https://dune.com/queries/5517435


SELECT
    date_trunc('day', block_time) AS date,
    blockchain,
    project,
    token_pair,
    SUM(amount_usd) AS volume
FROM dex.trades
WHERE block_time >= now() - interval '{{days}}' day
    AND lower(token_pair) LIKE lower('%{{token}}%')
    --AND blockchain IN ('monad', 'avalanche_c', 'ethereum')
    AND blockchain IN ('ethereum')
    AND project IN ('balancer','uniswap')
    AND amount_usd > 10
    
GROUP BY
    1, 2, 3, 4
ORDER BY
    date DESC,
    volume DESC