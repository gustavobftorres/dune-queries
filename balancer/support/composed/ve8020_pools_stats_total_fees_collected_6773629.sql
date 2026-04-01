-- part of a query repo
-- query name: ve8020 Pools Stats - Total fees collected
-- query link: https://dune.com/queries/6773629


WITH q AS (
    SELECT *
    FROM query_3108158
)
SELECT
    SUM(fees_collected_all_time) / 1e6 AS total_fees_collected
FROM q