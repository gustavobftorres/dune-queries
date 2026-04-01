-- part of a query repo
-- query name: by Chain
-- query link: https://dune.com/queries/2732262


SELECT 
    distinct
        CASE 
            WHEN blockchain = 'ethereum' THEN 1
            WHEN blockchain = 'arbitrum' THEN 2
            WHEN blockchain = 'polygon' THEN 3
            WHEN blockchain = 'optimism' THEN 4
            WHEN blockchain = 'gnosis' THEN 5
            WHEN blockchain = 'base' THEN 6
            WHEN blockchain = 'avalanche_c' THEN 7
        END AS order_num,
        blockchain || ' ' || ' ' ||  
        CASE 
            WHEN blockchain = 'arbitrum' THEN '| 🟦'
            WHEN blockchain = 'avalanche_c' THEN '| ⬜ '
            WHEN blockchain = 'base' THEN '| 🟨'
            WHEN blockchain = 'ethereum' THEN ' | Ξ'
            WHEN blockchain = 'gnosis' THEN '| 🟩'
            WHEN blockchain = 'optimism' THEN '| 🔴'
            WHEN blockchain = 'polygon' THEN '| 🟪'
        END AS blockchain, 
        CAST(block_date AS TIMESTAMP) AS block_date,
        CASE WHEN month(current_date) < 10 THEN substring(date_format(CAST(block_date AS TIMESTAMP), '%m-%d'), 2, 4)
           ELSE date_format(CAST(block_date AS TIMESTAMP), '%m-%d') 
        END AS formatted_block_date,
        approx_percentile(amount_usd, 0.5) OVER(PARTITION BY blockchain, block_date) AS median_vol,
        sum(amount_usd) OVER(PARTITION BY blockchain, block_date) AS sum_vol,
        count(*) OVER(PARTITION BY blockchain, block_date) AS num_trades,
        daily_traders
FROM balancer.trades x
LEFT JOIN (
    SELECT distinct 
        block_date AS b_date
        , blockchain AS b_chain
        , count(*) OVER(PARTITION BY blockchain, block_date) AS daily_traders
    FROM (
        SELECT distinct block_date, blockchain, tx_from FROM balancer.trades
        UNION
        SELECT distinct block_date, blockchain, tx_to FROM balancer.trades
    )
) y 
ON y.b_date = x.block_date
    AND y.b_chain = x.blockchain
WHERE amount_usd IS NOT NULL 
AND block_date >= current_date - interval '{{Date Range in Days}}' day
ORDER BY order_num ASC