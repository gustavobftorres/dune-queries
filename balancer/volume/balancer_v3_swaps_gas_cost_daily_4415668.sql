-- part of a query repo
-- query name: Balancer V3 Swaps Gas Cost, daily
-- query link: https://dune.com/queries/4415668


WITH
  txns as(
    SELECT DISTINCT hash, tx.gas_used, 'ethereum' as blockchain, 
        CASE WHEN (w.evt_index != 0 OR u.evt_index != 0)
        THEN 'yes'
        ELSE 'no'
        END AS wrap_unwrap,
    date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(s.pool,1,20) as pool_id
    FROM balancer_v3_ethereum.Vault_evt_Swap AS s
    INNER JOIN ethereum.transactions AS tx ON s.evt_tx_hash = tx.hash
    LEFT JOIN balancer_v3_ethereum.Vault_evt_Wrap AS w ON w.evt_tx_hash = s.evt_tx_hash
    LEFT JOIN balancer_v3_ethereum.Vault_evt_Unwrap AS u ON u.evt_tx_hash = s.evt_tx_hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    AND (w.evt_block_time IS NOT NULL OR u.evt_block_time IS NOT NULL)
    --AND "to" = 0xbA1333333333a1BA1108E8412f11850A5C319bA9
    
    UNION ALL 
    
    SELECT DISTINCT hash, tx.gas_used, 'gnosis' as blockchain, 
    CASE WHEN (w.evt_index != 0 OR u.evt_index != 0) IS NOT NULL
        THEN 'yes'
        ELSE 'no'
        END AS wrap_unwrap,
    date_trunc('day', s.evt_block_time) as day
        , BYTEARRAY_SUBSTRING(pool,1,20) as pool_id
    FROM balancer_v3_gnosis.Vault_evt_Swap AS s
    INNER JOIN gnosis.transactions AS tx ON s.evt_tx_hash = tx.hash
    LEFT JOIN balancer_v3_gnosis.Vault_evt_Wrap AS w ON w.evt_tx_hash = s.evt_tx_hash
    LEFT JOIN balancer_v3_gnosis.Vault_evt_Unwrap AS u ON u.evt_tx_hash = s.evt_tx_hash
    WHERE tx.block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day) 
    AND s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
),

  swaps AS (
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'ethereum' as blockchain
    FROM balancer_v3_ethereum.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1
    
    UNION ALL 
    
    SELECT evt_tx_hash, COUNT(1) AS n_swaps, 'gnosis' as blockchain
    FROM balancer_v3_gnosis.Vault_evt_Swap AS s
    WHERE s.evt_block_time > (CURRENT_DATE - INTERVAL '{{Timeframe, days}}' day)
    GROUP BY 1 
    ),
    
  gas_per_swap AS (
    SELECT
      txns.day,
      txns.blockchain,
      txns.pool_id,
      txns.hash,
      wrap_unwrap,
      gas_used,
      n_swaps,
      gas_used / CAST(n_swaps AS DOUBLE) AS gas_per_swap
    FROM
      txns
      INNER JOIN swaps ON swaps.evt_tx_hash = txns.hash AND swaps.blockchain = txns.blockchain
  )

SELECT
  CAST(day as DATE) as day,
  day as day_time,
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
g.wrap_unwrap,
  APPROX_PERCENTILE(gas_per_swap, 0.01) AS p01_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.05) AS p05_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.1) AS p10_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.25) AS p25_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.5) AS p50_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.75) AS p75_gas_per_swap,
  APPROX_PERCENTILE(gas_per_swap, 0.90) AS p90_gas_per_swap
FROM
  gas_per_swap g
 LEFT JOIN labels.balancer_v3_pools as q ON q.address = g.pool_id 
 AND q.blockchain = g.blockchain
 WHERE /*n_swaps = 1
 AND*/ ('{{Pool Type}}'= 'All' OR q.pool_type = '{{Pool Type}}')
 AND ('{{blockchain}}' = 'All' or g.blockchain = '{{blockchain}}')
 GROUP BY 1,2,3,4
 ORDER BY 1 DESC, 9 DESC --order by day and median gas_per_swap