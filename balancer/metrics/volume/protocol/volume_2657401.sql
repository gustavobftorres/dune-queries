-- part of a query repo
-- query name: Volume
-- query link: https://dune.com/queries/2657401


WITH base AS (
    SELECT
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '1' DAY THEN amount_usd END)  AS v_1d,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '7' DAY THEN amount_usd END)  AS v_7d,
        SUM(CASE WHEN block_time >= NOW() - INTERVAL '30' DAY THEN amount_usd END) AS v_30d,
        SUM(amount_usd)                                                             AS v_all,
        COUNT(CASE WHEN block_time >= NOW() - INTERVAL '1' DAY THEN 1 END)         AS trades_24h
    FROM dex.trades
    WHERE project = 'balancer'
)
SELECT 1 AS counter_num, concat('$', format_number(v_1d))  AS counter_metric FROM base UNION ALL
SELECT 2,                 concat('$', format_number(v_7d))                    FROM base UNION ALL
SELECT 3,                 concat('$', format_number(v_30d))                   FROM base UNION ALL
SELECT 4,                 concat('$', format_number(v_all))                   FROM base UNION ALL
SELECT 5,                 CAST(trades_24h AS VARCHAR)                         FROM base
ORDER BY counter_num