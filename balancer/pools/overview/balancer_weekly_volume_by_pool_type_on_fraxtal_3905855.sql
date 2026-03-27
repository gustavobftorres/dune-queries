-- part of a query repo
-- query name: Balancer Weekly Volume by Pool Type on Fraxtal
-- query link: https://dune.com/queries/3905855


    SELECT
        DATE_TRUNC('week', CAST(day AS TIMESTAMP)) AS week,
        pool_type,
        SUM(amount_usd) AS volume
    FROM dune.balancer.dataset_fraxtal_snapshots t
    WHERE CAST(day AS TIMESTAMP) >=   TIMESTAMP '{{1. Start date}}' 
    GROUP BY 1, 2
