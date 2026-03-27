-- part of a query repo
-- query name: Balancer Weekly Volume by Pool Type on Mode
-- query link: https://dune.com/queries/3906011


    SELECT
        DATE_TRUNC('week', CAST(day AS TIMESTAMP)) AS week,
        pool_type,
        SUM(amount_usd) AS volume
    FROM dune.balancer.dataset_mode_snapshots t
    WHERE CAST(day AS TIMESTAMP) >=   TIMESTAMP '{{1. Start date}}' 
    GROUP BY 1, 2
