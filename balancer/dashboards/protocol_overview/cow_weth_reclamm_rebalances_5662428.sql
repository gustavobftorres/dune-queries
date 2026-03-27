-- part of a query repo
-- query name: COW/WETH reCLAMM Rebalances
-- query link: https://dune.com/queries/5662428


WITH virtual_balance_events AS (
   SELECT 
       evt_block_time,
       evt_tx_hash,
       virtualBalanceA / 1e18 as virtual_balance_a,
       virtualBalanceB / 1e18 as virtual_balance_b,
       LAG(evt_block_time) OVER (ORDER BY evt_block_time) as prev_time
   FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated
   WHERE chain = 'base'
   AND contract_address = 0xff028c1ec4559d3aa2b0859aa582925b5cc28069
   ORDER BY 1
),

rebalancing_periods AS (
   SELECT 
       evt_block_time,
       evt_tx_hash,
       virtual_balance_a,
       virtual_balance_b,
       CASE 
           WHEN prev_time IS NULL THEN 1
           WHEN DATE_DIFF('hour', prev_time, evt_block_time) > 1 THEN 1
           ELSE 0
       END as is_new_period
   FROM virtual_balance_events
),

periods_grouped AS (
   SELECT 
       *,
       SUM(is_new_period) OVER (ORDER BY evt_block_time) as period_id
   FROM rebalancing_periods
)

SELECT 
   MIN(evt_block_time) as start_time,
   MAX(evt_block_time) as end_time,
   DATE_DIFF('minute', MIN(evt_block_time), MAX(evt_block_time)) / 60.0 as duration_hours,
   MIN(evt_tx_hash) as first_transaction,
   MAX(evt_tx_hash) as last_transaction
FROM periods_grouped
GROUP BY period_id
HAVING DATE_DIFF('minute', MIN(evt_block_time), MAX(evt_block_time)) > 0
ORDER BY 1 DESC
