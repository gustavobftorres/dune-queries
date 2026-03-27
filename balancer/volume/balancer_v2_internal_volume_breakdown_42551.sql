-- part of a query repo
-- query name: Balancer V2 Internal Volume Breakdown
-- query link: https://dune.com/queries/42551


SELECT
    date_trunc('day', evt_block_time) AS day,
    user::bytea,
    SUM(ABS(delta))
FROM balancer_v2."Vault_evt_InternalBalanceChanged"
GROUP BY  1, 2