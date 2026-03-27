-- part of a query repo
-- query name: Balancer Swaps
-- query link: https://dune.com/queries/31199


SELECT week, version, COUNT(*) AS swaps FROM (
SELECT
    date_trunc('week', evt_block_time) AS week,
    '1' AS version
FROM balancer."BPool_evt_LOG_SWAP"
UNION ALL
SELECT
    date_trunc('week', evt_block_time) AS week,
    '2' AS version
FROM balancer_v2."Vault_evt_Swap"
) s
GROUP BY 1,2
