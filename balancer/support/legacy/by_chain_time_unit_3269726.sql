-- part of a query repo
-- query name: by Chain & Time Unit
-- query link: https://dune.com/queries/3269726


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
            WHEN blockchain = 'zkevm' THEN 8
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
            WHEN blockchain = 'zkevm' THEN '| 🟪'
        END AS blockchain, 
        CAST(date_trunc('{{Time Unit}}', block_date) AS TIMESTAMP) AS block_date,
        approx_percentile(amount_usd, 0.5) OVER(PARTITION BY blockchain, date_trunc('{{Time Unit}}', block_date)) AS median_vol,
        sum(amount_usd) OVER(PARTITION BY blockchain, date_trunc('{{Time Unit}}', block_date)) AS sum_vol,
        count(*) OVER(PARTITION BY blockchain, date_trunc('{{Time Unit}}', block_date)) AS num_trades,
        daily_traders
FROM balancer.trades x
LEFT JOIN (
    SELECT distinct 
        block_date AS b_date
        , blockchain AS b_chain
        , count(*) OVER(PARTITION BY blockchain, block_date) AS daily_traders
    FROM (
        SELECT distinct date_trunc('{{Time Unit}}', block_date) AS block_date, blockchain, tx_from FROM balancer.trades
        UNION
        SELECT distinct date_trunc('{{Time Unit}}', block_date) AS block_date, blockchain, tx_to FROM balancer.trades
        WHERE 
            CASE 
                WHEN '{{Time Unit}}' = 'DAY' THEN block_date >= current_date - interval '{{Date Range in Time Units}}' DAY
                WHEN '{{Time Unit}}' = 'WEEK' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' DAY) * 7
                WHEN '{{Time Unit}}' = 'MONTH' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - interval '{{Date Range in Time Units}}' MONTH
                WHEN '{{Time Unit}}' = 'QUARTER' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' MONTH * 3)
            END
    )
) y 
ON y.b_date = x.block_date
    AND y.b_chain = x.blockchain
WHERE amount_usd IS NOT NULL 
    AND 
        CASE 
            WHEN '{{Time Unit}}' = 'DAY' THEN block_date >= current_date - interval '{{Date Range in Time Units}}' DAY
            WHEN '{{Time Unit}}' = 'WEEK' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' DAY) * 7
            WHEN '{{Time Unit}}' = 'MONTH' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - interval '{{Date Range in Time Units}}' MONTH
            WHEN '{{Time Unit}}' = 'QUARTER' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' MONTH * 3)
        END
ORDER BY order_num ASC