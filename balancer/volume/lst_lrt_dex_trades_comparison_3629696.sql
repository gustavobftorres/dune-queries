-- part of a query repo
-- query name: LST / LRT DEX Trades Comparison
-- query link: https://dune.com/queries/3629696


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
)
SELECT 
    CASE WHEN '{{Aggregation}}' = 'daily' THEN block_date
         WHEN '{{Aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
         WHEN '{{Aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS block_date, 
    CASE WHEN t.project IN (SELECT project FROM top_10_projects) THEN t.project
         ELSE 'others'
    END AS project,
    SUM(amount_usd) AS total_volume
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
