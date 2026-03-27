-- part of a query repo
-- query name: ve8020 Pools Stats - Total Liquidity
-- query link: https://dune.com/queries/6773587


WITH q AS (
    SELECT *
    FROM query_3108158
)
SELECT
    SUM(tvl) / 1e6 AS total_liquidity
FROM q