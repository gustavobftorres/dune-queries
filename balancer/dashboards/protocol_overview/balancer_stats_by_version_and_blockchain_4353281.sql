-- part of a query repo
-- query name: Balancer Stats by Version and Blockchain
-- query link: https://dune.com/queries/4353281


    SELECT 
        block_date,
        version,
        SUM(tvl_usd) AS daily_tvl_usd,
        SUM(tvl_eth) AS daily_tvl_eth,
        SUM(swap_amount_usd) AS daily_volume_usd,
        SUM(fee_amount_usd) AS daily_fees_usd
    FROM balancer.pools_metrics_daily
    WHERE block_date >= TIMESTAMP '2024-11-29 00:00'
    AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
    GROUP BY 1, 2
