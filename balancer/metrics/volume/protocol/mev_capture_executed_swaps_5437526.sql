-- part of a query repo
-- query name: MEV Capture - Executed Swaps
-- query link: https://dune.com/queries/5437526


SELECT 
    t.block_time,
    t.block_number,
    token_bought_symbol AS token_bought,
    token_sold_symbol AS token_sold,
    token_bought_amount,
    token_sold_amount,
    0.0001 AS static_swap_fee,
    swap_fee AS dynamic_swap_fee,
    (0.0001 != swap_fee) AS has_dynamic_fee,
    0.0001 * amount_usd AS static_swap_fee_usd,
    swap_fee * amount_usd AS dynamic_swap_fee_usd,
    TRY(tx_fee_breakdown['base_fee']) * 1e9  AS base_fee,
    TRY(tx_fee_breakdown['priority_fee']) * 1e9  AS priority_fee,
    amount_usd,
    t.tx_hash
FROM balancer.trades t
JOIN gas_base.fees g
ON g.tx_hash = t.tx_hash
WHERE t.project_contract_address = 0xd0bfa4784285acd49e06e12f302c2441c5923bfd
AND t.blockchain = 'base'
AND t.version = '3'
ORDER BY 1 DESC
