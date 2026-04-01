-- part of a query repo
-- query name: Cumulative Fees
-- query link: https://dune.com/queries/3478729


WITH 
pool_labels AS(
    SELECT
        blockchain,
        address,
        CASE WHEN pool_type IN ('WP', 'WP2T')
        THEN 'Weighted'
        WHEN pool_type IN ('SP')
        THEN 'Stable'
        WHEN pool_type IN ('IP')
        THEN 'Investment'   
        WHEN pool_type IN ('MP')
        THEN 'Managed'
        ELSE pool_type
        END AS pool_type
    FROM labels.balancer_v2_pools
    ),
fees AS (
  SELECT
    CASE WHEN 
        '{{Aggregation}}' = 'Monthly'
    THEN CAST(DATE_TRUNC('month', day) AS TIMESTAMP) 
    WHEN 
        '{{Aggregation}}' = 'Weekly'
    THEN CAST(DATE_TRUNC('week', day) AS TIMESTAMP) 
        WHEN 
        '{{Aggregation}}' = 'Daily'
    THEN CAST(DATE_TRUNC('day', day) AS TIMESTAMP) 
    END AS date,
    SUM(protocol_fee_collected_usd) AS fees
  FROM balancer.protocol_fee f
  LEFT JOIN dune.balancer.dataset_core_pools c 
  ON c.network = f.blockchain AND c.pool = f.pool_id
  LEFT JOIN pool_labels l 
  ON l.blockchain = f.blockchain AND l.address = f.pool_address
  WHERE ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
  AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
  AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
  AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
  GROUP BY 1
)

SELECT
  date,
  fees AS total_fees,
  SUM(fees) OVER (ORDER BY date) AS cumulative_fees
FROM fees