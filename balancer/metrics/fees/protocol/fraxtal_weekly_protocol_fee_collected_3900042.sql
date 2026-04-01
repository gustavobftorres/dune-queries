-- part of a query repo
-- query name: Fraxtal Weekly Protocol Fee Collected
-- query link: https://dune.com/queries/3900042


    SELECT
        DATE_TRUNC('week', CAST(day AS TIMESTAMP)) AS week,
        pool_symbol,
        SUM(CAST(protocol_fee_collected_usd AS DOUBLE)) AS protocol_fee_collected
    FROM
        dune.balancer.dataset_fraxtal_snapshots s
        WHERE CAST(day AS TIMESTAMP) >=   TIMESTAMP '{{1. Start date}}' 
    GROUP BY 1, 2
    ORDER BY 1 DESC