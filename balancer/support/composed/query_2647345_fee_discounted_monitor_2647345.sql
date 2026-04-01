-- part of a query repo
-- query name: (query_2647345) fee_discounted_monitor
-- query link: https://dune.com/queries/2647345


/*
queried on: 
Fee Discounted Volume by chain, last 14 days https://dune.com/queries/2828264
Fee Discounted Volume by aggregator, last 14 days https://dune.com/queries/2647969
Fee Discounted Aggregator https://dune.com/queries/2720447
*/
WITH
  fee_changes AS (
    SELECT a.*
    FROM balancer_v2_ethereum.pools_fees a
    UNION ALL
    SELECT b.*
    FROM balancer_v2_arbitrum.pools_fees b
    UNION ALL
    SELECT c.*
    FROM balancer_v2_polygon.pools_fees c
  ),
  
  swaps AS (
    SELECT *
    FROM dex.trades
    WHERE project = 'balancer' AND tx_to IN(0x9008d19f58aabd9ed0d60971565aa8510560ab41, 0xad3b67bca8935cb510c8d18bd45f0b94f54a968f) AND block_time > TIMESTAMP '2024-01-01 00:00'
  ),    

  side_by_side_fee_changes AS (
    SELECT
      contract_address, block_number, block_time, index, f.blockchain,
      LAG(swap_fee_percentage) OVER (PARTITION BY contract_address ORDER BY ARRAY[block_number] || index) / 1e18 AS prev_fee,
      swap_fee_percentage / 1e18 AS fee,
      LEAD(swap_fee_percentage) OVER (PARTITION BY contract_address ORDER BY ARRAY[block_number] || index) / 1e18 AS next_fee,
      LAG(tx_hash) OVER (PARTITION BY contract_address ORDER BY ARRAY[block_number] || index) AS prev_fee_tx_hash,
      tx_hash,
      LEAD(tx_hash) OVER (PARTITION BY contract_address ORDER BY ARRAY[block_number] || index) AS next_fee_tx_hash
    FROM fee_changes f
  ),
  classification AS (
    SELECT 
      CASE
        WHEN fee < prev_fee -- Changed from ">" to "<" to detect lowered fees
          THEN 'LOWERED FEE'
        ELSE 'OK'
      END AS check,
      s.amount_usd,
      s.tx_to,
      f.*,
    CASE WHEN f.blockchain = 'ethereum' THEN CONCAT('<a target="_blank" href="https://etherscan.io/tx/', CAST(s.tx_hash as VARCHAR), '">Scan ↗</a>')
    WHEN f.blockchain = 'arbitrum' THEN CONCAT('<a target="_blank" href="https://arbiscan.io/tx/', CAST(s.tx_hash as VARCHAR), '">Scan ↗</a>')
    WHEN f.blockchain = 'polygon' THEN CONCAT('<a target="_blank" href="https://polygonscan.com/tx/', CAST(s.tx_hash as VARCHAR), '">Scan ↗</a>')
    END AS etherscan
    FROM side_by_side_fee_changes f
    LEFT JOIN swaps s ON s.tx_hash = f.tx_hash AND s.blockchain = f.blockchain
    WHERE prev_fee IS NOT NULL
    ORDER BY
      block_time DESC,
      contract_address,
      block_number,
      index
  )
SELECT
    blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN blockchain = 'base' THEN ' 🟨'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'optimism' THEN ' 🔴'
            WHEN blockchain = 'polygon' THEN ' 🟪'
        END 
    AS blockchain_symbol,
  *,
  CASE
    WHEN tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
      THEN CONCAT('<a target="_blank" href="https://snapshot.org/#/balancer.eth/proposal/0xd991e9f3c6edd148bd37c600d7ada3d28db1758e3cfd703c02d290f502906f05">', 'CowSwap ↗</a>')
    WHEN tx_to = 0xad3b67bca8935cb510c8d18bd45f0b94f54a968f
      THEN CONCAT('<a target="_blank" href="https://snapshot.org/#/balancer.eth/proposal/0x345e0618bd258a0f79ae05afe8097533f7a0142f250aced1146c123698e8a9dc">', '1inch ↗</a>')
    ELSE ''
  END AS "Discount Reason"
FROM
  classification c
WHERE check = 'LOWERED FEE'
  AND amount_usd IS NOT NULL -- eliminating txs where there were only SwapFeePercentageChanged e.g. https://etherscan.io/tx/0x6a0f413d628e8f8229f4a182cfd7f8863dbf97f879ef9b0435be8fdda3d2550d/advanced#eventlog
ORDER BY block_time DESC;
