-- part of a query repo
-- query name: V3 Protocol Fees Buffering
-- query link: https://dune.com/queries/4650222


SELECT
    day,
    blockchain || 
        CASE 
            WHEN blockchain = 'arbitrum' THEN ' 🟦'
            WHEN blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN blockchain = 'base' THEN ' 🟨'
            WHEN blockchain = 'ethereum' THEN ' Ξ'
            WHEN blockchain = 'gnosis' THEN ' 🟩'
            WHEN blockchain = 'optimism' THEN ' 🔴'
            WHEN blockchain = 'polygon' THEN ' 🟪'
            WHEN blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain,
    pool,
    pool_symbol,
    token_symbol,
    protocol_yield_fee_vault_balance_usd + protocol_swap_fee_vault_balance_usd AS total_fee_vault_usd,
    SUM(COALESCE(protocol_yield_fee_vault_balance_usd, 0)) AS protocol_yield_fee_vault_usd,
    SUM(COALESCE(protocol_swap_fee_vault_balance_usd, 0)) AS protocol_swap_fee_vault_usd,
    SUM(COALESCE(protocol_yield_fee_vault_balance, 0)) AS protocol_yield_fee_vault,
    SUM(COALESCE(protocol_swap_fee_vault_balance, 0)) AS protocol_swap_fee_vault
FROM dune.balancer.result_protocol_fee_pool_snapshots
WHERE day = CURRENT_DATE - INTERVAL '1' day
AND ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1 DESC, 6 DESC