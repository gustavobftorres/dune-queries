-- part of a query repo
-- query name: Mode Weekly Protocol Fee Collected
-- query link: https://dune.com/queries/3906025


    SELECT
        DATE_TRUNC('week', CAST(day AS TIMESTAMP)) AS week,
        pool_symbol,
        SUM(protocol_fee_collected_usd) AS protocol_fee_collected
    FROM
        dune.balancer.dataset_mode_snapshots s
        WHERE CAST(day AS TIMESTAMP) >=   TIMESTAMP '{{1. Start date}}' 
    GROUP BY 1, 2
    ORDER BY 1 DESC