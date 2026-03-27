-- part of a query repo
-- query name: DEX Relative Weight of LSTs / LRTs on Trades
-- query link: https://dune.com/queries/3950369


WITH ranked_volumes AS (
    SELECT 
        project, 
        SUM(amount_usd) AS total_volume
    FROM dex.trades t
    INNER JOIN dune.balancer.result_lst_tokens l 
        ON t.blockchain = l.blockchain 
        AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE t.blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND block_date >= CURRENT_DATE - INTERVAL '30' DAY
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY project
),
top_10_projects AS (
    SELECT project
    FROM ranked_volumes
    ORDER BY total_volume DESC
    LIMIT 10
),
total_volumes AS (
    SELECT 
        CASE WHEN '{{Aggregation}}' = 'daily' THEN block_date
             WHEN '{{Aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
             WHEN '{{Aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
        END AS block_date, 
        CASE WHEN project IN (SELECT project FROM top_10_projects) THEN project
             ELSE 'others'
        END AS project,
        SUM(amount_usd) AS total_volume
    FROM dex.trades
    WHERE blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND block_date >= TIMESTAMP '{{Start Date}}'
    AND ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY 
        CASE WHEN '{{Aggregation}}' = 'daily' THEN block_date
             WHEN '{{Aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
             WHEN '{{Aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
        END, 
        project
),
lst_volumes AS (
    SELECT 
        CASE WHEN '{{Aggregation}}' = 'daily' THEN block_date
             WHEN '{{Aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
             WHEN '{{Aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
        END AS block_date, 
        CASE WHEN t.project IN (SELECT project FROM top_10_projects) THEN t.project
             ELSE 'others'
        END AS project,
        SUM(amount_usd) AS lst_volume
    FROM dex.trades t
    INNER JOIN dune.balancer.result_lst_tokens l 
        ON t.blockchain = l.blockchain 
        AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE t.blockchain IN (SELECT DISTINCT blockchain FROM balancer.liquidity)
    AND block_date >= TIMESTAMP '{{Start Date}}'
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND ('{{Token Pair}}' = 'All' OR token_pair = '{{Token Pair}}')
    GROUP BY 
        CASE WHEN '{{Aggregation}}' = 'daily' THEN block_date
             WHEN '{{Aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
             WHEN '{{Aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
        END, 
        CASE WHEN t.project IN (SELECT project FROM top_10_projects) THEN t.project
             ELSE 'others'
        END
)
SELECT 
    t.block_date, 
    t.project,
    t.total_volume,
    l.lst_volume,
    (l.lst_volume / t.total_volume) AS lst_percentage
FROM total_volumes t
LEFT JOIN lst_volumes l
    ON t.block_date = l.block_date 
    AND t.project = l.project
AND t.project != 'others'
WHERE l.lst_volume < t.total_volume
ORDER BY t.block_date, t.project