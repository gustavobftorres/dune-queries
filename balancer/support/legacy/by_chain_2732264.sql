-- part of a query repo
-- query name: by Chain
-- query link: https://dune.com/queries/2732264


SELECT 
    distinct
        CASE 
            WHEN blockchain = 'ethereum' THEN 1
            WHEN blockchain = 'arbitrum' THEN 2
            WHEN blockchain = 'polygon' THEN 3
            WHEN blockchain = 'optimism' THEN 4
            WHEN blockchain = 'gnosis' THEN 5
        END AS order_num,
        blockchain, 
        block_date,
        approx_percentile(amount_usd, 0.5) OVER(PARTITION BY blockchain, block_date) AS median_vol,
        sum(amount_usd) OVER(PARTITION BY blockchain, block_date) AS sum_vol,
        count(*) OVER(PARTITION BY blockchain, block_date) AS num_trades
FROM balancer.trades 
WHERE amount_usd IS NOT NULL 
AND block_date >= current_date - interval '7' day
ORDER BY order_num ASC--, median_vol DESC, sum_vol DESC block_date DESC, 