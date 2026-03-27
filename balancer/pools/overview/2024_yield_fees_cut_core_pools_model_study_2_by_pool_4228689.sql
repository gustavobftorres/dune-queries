-- part of a query repo
-- query name: 2024 Yield Fees Cut + Core Pools model Study 2, by pool
-- query link: https://dune.com/queries/4228689


WITH fees AS (
    SELECT 
        CASE WHEN c.symbol IS NOT NULL THEN 'Core' ELSE 'Non-Core' END AS category,
        blockchain,
        block_month,
        pool_address,
        pool_symbol,
        SUM(total_protocol_yield_fee) AS yield_fee,
        SUM(total_protocol_swap_fee) AS swap_fee
    FROM query_4104279 q
    LEFT JOIN dune.balancer.dataset_core_pools c
    ON q.blockchain = c.network
    AND q.pool_address = BYTEARRAY_SUBSTRING(c.pool, 1, 20)
    WHERE total_protocol_yield_fee >= 0 
    AND total_protocol_swap_fee >= 0
    AND block_month >= TIMESTAMP '2024-01-01'
    AND block_month < TIMESTAMP '2025-01-01'
    AND ('{{blockchain}}' = 'all' OR blockchain = '{{blockchain}}')
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    category,
    blockchain,
    block_month,
    pool_address,
    pool_symbol,
    -- Actual Fees
    -- SUM(yield_fee) AS actual_protocol_yield_fee,
    SUM(swap_fee + yield_fee) AS actual_protocol_fee
    -- SUM(yield_fee) AS actual_lp_yield_fee,
    -- SUM(swap_fee) AS actual_lp_swap_fee,
    -- SUM(yield_fee * 
    --     CASE 
    --         WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum') THEN 0.175 
    --         ELSE 0.0875 
    --     END) AS actual_dao_yield_fee,
    -- SUM(swap_fee * 
    --     CASE 
    --         WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum') THEN 0.175 
    --         ELSE 0.0875 
    --     END) AS actual_dao_swap_fee,
    -- SUM(yield_fee * 
    --     CASE 
    --         WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum') THEN 0.325 
    --         ELSE 0.4125 
    --     END) AS actual_vebal_yield_fee,
    -- SUM(swap_fee * 
    --     CASE 
    --         WHEN ((category = 'Core' AND blockchain = 'ethereum') OR blockchain != 'ethereum') THEN 0.325 
    --         ELSE 0.4125 
    --     END) AS actual_vebal_swap_fee,
    -- SUM(yield_fee * 0.5) AS actual_incentive_yield_fee,
    -- SUM(swap_fee * 0.5) AS actual_incentive_swap_fee,

    -- -- V3 (run V2 Core pools only with 10% Yield, no non-core redirect)
    -- --  Core pools: 70% incentives, 12.5% veBAL and 17.5%  DAO
    -- SUM(CASE WHEN category = 'Core' THEN yield_fee * {{cut_rate_in_yield_fees}} ELSE 0 END) AS proposed_10_protocol_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee ELSE 0 END) AS proposed_10_protocol_swap_fee,
    -- SUM(CASE WHEN category = 'Core' THEN yield_fee * (2 - {{cut_rate_in_yield_fees}}) ELSE 0 END) AS proposed_10_lp_yield_fee,    
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee ELSE 0 END) AS proposed_10_lp_swap_fee,   
    -- SUM(CASE WHEN category = 'Core' THEN (yield_fee * {{cut_rate_in_yield_fees}}) * 0.175 ELSE 0 END) AS proposed_10_dao_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.175 ELSE 0 END) AS proposed_10_dao_swap_fee,
    -- SUM(CASE WHEN category = 'Core' THEN (yield_fee * {{cut_rate_in_yield_fees}}) * 0.125 ELSE 0 END) AS proposed_10_vebal_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.125 ELSE 0 END) AS proposed_10_vebal_swap_fee,
    -- SUM(CASE WHEN category = 'Core' THEN (yield_fee * {{cut_rate_in_yield_fees}}) * 0.7 ELSE 0 END) AS proposed_10_incentive_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.7 ELSE 0 END) AS proposed_10_incentive_swap_fee,

    -- -- V2 numbers with 50% Yield feed ,but no non-core redirect:
    -- -- Core pools: 70% incentives, 12.5% veBAL and 17.5%  DAO
    -- -- Non-core pools: 82.5% veBAL, 17.5% DAO
    -- SUM(yield_fee) AS proposed_50_protocol_yield_fee,
    -- SUM(swap_fee) AS proposed_50_protocol_swap_fee,
    -- SUM(yield_fee) AS proposed_50_lp_yield_fee,
    -- SUM(swap_fee) AS proposed_50_lp_swap_fee,    
    -- SUM(CASE WHEN category = 'Core' THEN (yield_fee + swap_fee) * 0.175 ELSE (yield_fee + swap_fee) * 0.175 END) AS proposed_50_dao_fees,
    -- SUM(CASE WHEN category = 'Core' THEN yield_fee * 0.175 ELSE yield_fee * 0.175 END) AS proposed_50_dao_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.175 ELSE swap_fee * 0.175 END) AS proposed_50_dao_swap_fee,
    -- SUM(CASE WHEN category = 'Core' THEN yield_fee * 0.125 ELSE yield_fee * 0.825 END) AS proposed_50_vebal_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.125 ELSE swap_fee * 0.825 END) AS proposed_50_vebal_swap_fee,
    -- SUM(CASE WHEN category = 'Core' THEN yield_fee * 0.7 ELSE 0 END) AS proposed_50_incentive_yield_fee,
    -- SUM(CASE WHEN category = 'Core' THEN swap_fee * 0.7 ELSE 0 END) AS proposed_50_incentive_swap_fee

FROM fees
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4, 5
