-- part of a query repo
-- query name: 7D Volume
-- query link: https://dune.com/queries/6773472


SELECT DISTINCT
    project_contract_address AS pool,
    blockchain,
    SUM(amount_usd) OVER (PARTITION BY project_contract_address, blockchain) AS vol
FROM balancer.trades
WHERE block_time > NOW() - INTERVAL '7' day