-- part of a query repo
-- query name: trace_based_balancer_v3_swap_gas_cost_by_pool
-- query link: https://dune.com/queries/4644257


SELECT 
       s.pool_symbol,
       s.pool_type,
       min(t.gas_used) as min_gas,
       avg(t.gas_used) as avg_gas,
       max(t.gas_used) as max_gas,
       approx_percentile(t.gas_used, 0.5) as median_gas
FROM ethereum.traces t
JOIN balancer.trades s ON s.tx_hash = t.tx_hash
AND s.blockchain = 'ethereum'
AND s.version = '3'
WHERE "to" = {{router}}
AND substr(input, 1, 4) = {{signature}} -- swapSingleTokenExactIn
AND tx_success = true
AND success = true
AND t.block_time > CURRENT_TIMESTAMP - INTERVAL '3' MONTH
GROUP BY 1, 2