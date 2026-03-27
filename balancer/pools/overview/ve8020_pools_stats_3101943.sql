-- part of a query repo
-- query name: ve8020 Pools Stats
-- query link: https://dune.com/queries/3101943


WITH q AS (
    SELECT *
    FROM query_3108158
)
SELECT
    SUM(CASE WHEN t.block_time > now() - interval '24' hour THEN t.amount_usd ELSE 0 END) / 1e6 AS volume_24h,
    SUM(CASE WHEN t.block_time > now() - interval '7'  day  THEN t.amount_usd ELSE 0 END) / 1e6 AS volume_7d,
    SUM(CASE WHEN t.block_time > now() - interval '30' day  THEN t.amount_usd ELSE 0 END) / 1e6 AS volume_30d,
    SUM(t.amount_usd) / 1e9                                                                      AS volume_agg,
    pool_totals.fees / 1e6                                                                        AS fees_collected_all_time,
    pool_totals.tvl  / 1e6                                                                        AS tvl
FROM balancer.trades t
LEFT JOIN q ON CAST(t.project_contract_address AS VARCHAR) = q.address
           AND t.blockchain = q.blockchain
CROSS JOIN (
    SELECT SUM(fees_collected_all_time) AS fees, SUM(tvl) AS tvl
    FROM q
) pool_totals
WHERE q.project IS NOT NULL
GROUP BY pool_totals.fees, pool_totals.tvl