-- part of a query repo
-- query name: LP vs. Protocol Fees
-- query link: https://dune.com/queries/4664084


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
    SUM(lp_fee_collected_usd) AS lp_fees,
    SUM(protocol_fee_collected_usd) AS protocol_fees
  FROM balancer.protocol_fee f
  LEFT JOIN dune.balancer.dataset_core_pools c 
  ON c.network = f.blockchain AND c.pool = f.pool_id
  WHERE ('{{Pool Address}}' = 'All' OR CAST(pool_address AS VARCHAR) = '{{Pool Address}}')
  AND ('{{Blockchain}}' = 'All' OR f.blockchain = '{{Blockchain}}')
  AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
  AND ('{{Pool Type}}' = 'All' OR f.pool_type = '{{Pool Type}}')
  GROUP BY 1