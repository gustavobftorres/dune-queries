-- part of a query repo
-- query name: Linear Pool - Mints
-- query link: https://dune.com/queries/3358333


SELECT DATE_TRUNC('day', evt_block_time) as block_date, SUM(value/POWER(10,18)) as ajoins
FROM test_schema.git_dunesql_c6a21c9e_balancer_v2_arbitrum_transfers_bpt
WHERE contract_address = 0x7c82a23b4c48d796dee36a9ca215b641c6a8709d
AND "from" = 0x0000000000000000000000000000000000000000
GROUP BY 1