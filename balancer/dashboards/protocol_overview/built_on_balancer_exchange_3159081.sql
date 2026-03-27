-- part of a query repo
-- query name: Built on Balancer Exchange
-- query link: https://dune.com/queries/3159081


WITH
    project_data as(
    SELECT * FROM query_3144841
    WHERE project = '{{Project}}'
    ),
    
  swaps AS (
    SELECT
     date_trunc('week', block_time) as week,
      COUNT(*) AS transactions,
      SUM(CAST(amount_usd AS DOUBLE)) AS volume
    FROM
      balancer.trades d 
            LEFT JOIN project_data p ON p.blockchain = d.blockchain
                AND BYTEARRAY_SUBSTRING(p.pool_id,1,20) = d.project_contract_address
         WHERE name IS NOT NULL
            AND d.block_time >= TIMESTAMP '{{Start Date}}' 
    GROUP BY 1
    
    UNION ALL
    
    SELECT
     date_trunc('week', block_time) as week,
      COUNT(*) AS transactions,
      SUM(CAST(amount_usd AS DOUBLE)) AS volume
    FROM
      beethoven_x.trades d 
            LEFT JOIN project_data p ON p.blockchain = d.blockchain
                AND BYTEARRAY_SUBSTRING(p.pool_id,1,20) = d.project_contract_address
         WHERE name IS NOT NULL
            AND d.block_time >= TIMESTAMP '{{Start Date}}' 
    GROUP BY 1  
    
    UNION ALL
    
    SELECT
     date_trunc('week', block_time) as week,
      COUNT(*) AS transactions,
      SUM(CAST(amount_usd AS DOUBLE)) AS volume
    FROM
      jelly_swap.trades d 
            LEFT JOIN project_data p ON p.blockchain = d.blockchain
                AND BYTEARRAY_SUBSTRING(p.pool_id,1,20) = d.project_contract_address
         WHERE name IS NOT NULL
            AND d.block_time >= TIMESTAMP '{{Start Date}}' 
    GROUP BY 1      
    )
    
SELECT
  week,
  transactions,
  volume,
  volume / transactions
FROM
  swaps