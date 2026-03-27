-- part of a query repo
-- query name: Balancer Swaps Aggregated - Materialized view
-- query link: https://dune.com/queries/6754098


-- Materialized view: balancer_swaps_aggregated (no parameters)
SELECT
    date_trunc('day', block_time) AS "date",  -- always store at daily grain
    token,
    blockchain,
    version,
    SUM(amount_usd) AS volume
FROM query_6754023
GROUP BY 1, 2, 3, 4