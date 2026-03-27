-- part of a query repo
-- query name: Balancer CoWSwap AMM Trades
-- query link: https://dune.com/queries/3965119


SELECT
    date_trunc('month', d.block_time) AS day,
    SUM(amount_usd) AS volume,
    AVG(amount_usd) AS avg_trade,
    COUNT(DISTINCT tx_from) AS traders
FROM balancer_cowswap_amm.trades d
WHERE blockchain = '{{4. Blockchain}}'
AND ('{{1. Pool Address}}' = 'All' OR project_contract_address = {{1. Pool Address}})
AND block_time >= TIMESTAMP '{{2. Start date}}'
AND block_time <= TIMESTAMP '{{3. End date}}'
GROUP BY 1