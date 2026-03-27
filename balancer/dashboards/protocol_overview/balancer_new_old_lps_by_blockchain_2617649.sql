-- part of a query repo
-- query name: Balancer New/Old LPs by Blockchain
-- query link: https://dune.com/queries/2617649


-- Number of new/old Liquidity Providers per week
-- Visualization: bar chart (stacked)

SELECT
    ssq.time, 
    new_users as "New",
    (unique_users - new_users) as "Old"
FROM (
    SELECT
        sq.time, 
        COUNT(*) as new_users
    FROM (
        SELECT
            "liquidityProvider" AS unique_users,
            MIN(date_trunc('week', evt_block_time)) AS time
        FROM balancer_v2_{{4. Blockchain}}.Vault_evt_PoolBalanceChanged
        GROUP BY 1
  ) sq
    GROUP BY 1
    ORDER BY 1
) ssq
LEFT JOIN (
    SELECT
        date_trunc('week', evt_block_time) AS time,
        COUNT(DISTINCT caller) AS unique_users
        FROM (SELECT "liquidityProvider" AS caller, evt_block_time FROM balancer_v2_polygon.Vault_evt_PoolBalanceChanged) foo
    GROUP BY 1
    ORDER BY 1
) t2 ON t2.time = ssq.time
WHERE (ssq.time >= TIMESTAMP '{{2. Start date}}' AND ssq.time <= TIMESTAMP '{{3. End date}}')
OR (t2.time>= TIMESTAMP '{{2. Start date}}' AND t2.time <= TIMESTAMP '{{3. End date}}')
ORDER BY 1 DESC