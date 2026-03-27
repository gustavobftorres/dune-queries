-- part of a query repo
-- query name: Balancer Trades
-- query link: https://dune.com/queries/2718062


SELECT
    date_trunc('month', d.block_time) AS day,
    SUM(amount_usd) AS volume,
    AVG(amount_usd) AS avg_trade,
    COUNT(DISTINCT tx_from) AS traders
FROM balancer.trades d
WHERE blockchain = '{{4. Blockchain}}'
AND ('{{1. Pool ID}}' = 'All' OR CAST(project_contract_address as VARCHAR) = SUBSTRING('{{1. Pool ID}}',1,42))
AND block_time >= TIMESTAMP '{{2. Start date}}'
AND block_time <= TIMESTAMP '{{3. End date}}'
GROUP BY 1