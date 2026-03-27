-- part of a query repo
-- query name: wstETH-WETH swaps above 100k USD
-- query link: https://dune.com/queries/4221517


SELECT 
    block_date,
    count(*)
FROM balancer.trades
WHERE token_pair = 'WETH-wstETH'
AND amount_usd > 100000
GROUP BY 1
ORDER BY 1 DESC