-- part of a query repo
-- query name: Balancer V2 Batch Swaps Gas Cost per swap, Daily
-- query link: https://dune.com/queries/3092409


WITH
  txns as(
    SELECT DISTINCT hash, tx.gas_used, 'arbitrum' as blockchain, date_trunc('day', s.evt_block_time) as day
    , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_arbitrum.Vault_evt_Swap AS s
    INNER JOIN arbitrum.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL
    
    SELECT DISTINCT hash, tx.gas_used, 'avalanche_c' as blockchain, date_trunc('day', s.evt_block_time) as day
    , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_avalanche_c.Vault_evt_Swap AS s
    INNER JOIN avalanche_c.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'base' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_base.Vault_evt_Swap AS s
    INNER JOIN base.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'ethereum' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_ethereum.Vault_evt_Swap AS s
    INNER JOIN ethereum.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'gnosis' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_gnosis.Vault_evt_Swap AS s
    INNER JOIN gnosis.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'optimism' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_optimism.Vault_evt_Swap AS s
    INNER JOIN optimism.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'polygon' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_polygon.Vault_evt_Swap AS s
    INNER JOIN polygon.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'zkevm' as blockchain, date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(poolID,1,20) as pool_id
    FROM balancer_v2_zkevm.Vault_evt_Swap AS s
    INNER JOIN zkevm.transactions AS tx ON s.evt_tx_hash = tx.hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND "to" = 0xba12222222228d8ba445958a75a0704d566bf2c8    
),

  swaps AS (
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'avalanche_c' as blockchain
    FROM balancer_v2_avalanche_c.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1    
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'base' as blockchain
    FROM balancer_v2_base.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1     
  
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'ethereum' as blockchain
    FROM balancer_v2_ethereum.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'gnosis' as blockchain
    FROM balancer_v2_gnosis.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1 
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'optimism' as blockchain
    FROM balancer_v2_optimism.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1 
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'polygon' as blockchain
    FROM balancer_v2_polygon.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1 
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'zkevm' as blockchain
    FROM balancer_v2_zkevm.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1     
    ),
    
  gas_per_swap AS (
    SELECT
      txns.day,
      txns.blockchain,
      txns.pool_id,
      txns.hash,
      gas_used,
      n_swaps,
      gas_used / CAST(n_swaps AS DOUBLE) AS gas_per_swap
    FROM
      txns
      INNER JOIN swaps ON swaps.evt_tx_hash = txns.hash AND swaps.blockchain = txns.blockchain
     WHERE n_swaps > 1)

SELECT
  CAST(day as DATE) as day,
  day as day_time,
  n_swaps,
    g.blockchain || 
        CASE 
            WHEN g.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN g.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN g.blockchain = 'base' THEN ' 🟨'
            WHEN g.blockchain = 'ethereum' THEN ' Ξ'
            WHEN g.blockchain = 'gnosis' THEN ' 🟩'
            WHEN g.blockchain = 'optimism' THEN ' 🔴'
            WHEN g.blockchain = 'polygon' THEN ' 🟪'
            WHEN g.blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain,
  APPROX_PERCENTILE(gas_per_swap, 0.01) AS p01_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.05) AS p05_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.1) AS p10_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.25) AS p25_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.5) AS p50_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.75) AS p75_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.90) AS p90_gas_per_swap
FROM
  gas_per_swap g
 LEFT JOIN query_3093820 as q ON q.pool = g.pool_id 
 WHERE ('{{# of swaps}}' = 'Any' OR n_swaps = CAST('{{# of swaps}}' as BIGINT))
 AND ('{{Pool Type}}'= 'All' OR q.pool_type = '{{Pool Type}}')
 AND ('{{blockchain}}' = 'All' or g.blockchain = '{{blockchain}}')
 GROUP BY 1,2,3,4
 ORDER BY 1 DESC, 4, 3 ASC, 9 DESC --order by day and median gas_per_swap