-- part of a query repo
-- query name: 2024 Yield Fees Cut + Core Pools model Study
-- query link: https://dune.com/queries/4218397


WITH fees AS(
SELECT 
    CASE WHEN c.symbol IS NOT NULL
    THEN 'Core'
    ELSE 'Non-Core'
    END AS category,
    blockchain,
    SUM(total_protocol_yield_fee) AS yield_fee,
    SUM(total_protocol_swap_fee) AS swap_fee
FROM query_4104279 q
LEFT JOIN dune.balancer.dataset_core_pools c
ON q.blockchain = c.network
AND q.pool_address = BYTEARRAY_SUBSTRING(c.pool, 1, 20)
WHERE 1 = 1
AND total_protocol_yield_fee >= 0 
AND total_protocol_swap_fee >= 0
AND block_month >= TIMESTAMP '2024-01-01'
AND block_month < TIMESTAMP '2025-01-01'
AND ('{{blockchain}}' = 'all' OR blockchain = '{{blockchain}}')
GROUP BY 1, 2)

SELECT
    category,
    SUM(yield_fee + swap_fee) AS actual_protocol_fee,
    SUM(yield_fee * {{cut rate in yield fees}} + swap_fee) AS proposed_10_protocol_fee,
    SUM(yield_fee + swap_fee) AS actual_lp_fee,
    SUM(yield_fee * (2 - {{cut rate in yield fees}}) + swap_fee) AS proposed_10_lp_fee,    
    SUM(CASE WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum')
    THEN (yield_fee + swap_fee) * 0.175
    ELSE (yield_fee + swap_fee) * 0.175
    END) AS actual_dao_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN (yield_fee * {{cut rate in yield fees}} + swap_fee) * 0.5
    WHEN category = 'Core'
    THEN (yield_fee * {{cut rate in yield fees}} + swap_fee) * 0.125
    END) AS proposed_10_dao_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN (yield_fee + swap_fee) * 0.5
    WHEN category = 'Core'
    THEN (yield_fee + swap_fee) * 0.125
    END) AS proposed_50_dao_fees,    
    SUM(CASE WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum')
    THEN (yield_fee + swap_fee) * 0.325
    ELSE (yield_fee + swap_fee) * 0.4125
    END) AS actual_vebal_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN (yield_fee * {{cut rate in yield fees}} + swap_fee) * 0.5
    WHEN category = 'Core'
    THEN (yield_fee * {{cut rate in yield fees}} + swap_fee) * 0.125
    END) AS proposed_10_vebal_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN (yield_fee + swap_fee) * 0.5
    WHEN category = 'Core'
    THEN (yield_fee + swap_fee) * 0.125
    END) AS proposed_50_vebal_fees,    
    SUM(CASE WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum')
    THEN (yield_fee + swap_fee) * 0.5
    ELSE (yield_fee + swap_fee) * 0.5
    END) AS actual_incentive_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN 0
    WHEN category = 'Core'
    THEN (yield_fee * {{cut rate in yield fees}} + swap_fee) * 0.75
    END) AS proposed_10_incentive_fees,
    SUM(CASE WHEN category = 'Non-Core'
    THEN 0
    WHEN category = 'Core'
    THEN (yield_fee + swap_fee) * 0.75
    END) AS proposed_50_incentive_fees    
FROM fees
GROUP BY 1
ORDER BY 1 DESC