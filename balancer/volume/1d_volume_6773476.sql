-- part of a query repo
-- query name: 1D Volume
-- query link: https://dune.com/queries/6773476


SELECT DISTINCT
    project_contract_address AS pool,
    blockchain,
    SUM(amount_usd) OVER (PARTITION BY project_contract_address, blockchain) AS vol
FROM balancer.trades
WHERE block_time > NOW() - INTERVAL '1' day