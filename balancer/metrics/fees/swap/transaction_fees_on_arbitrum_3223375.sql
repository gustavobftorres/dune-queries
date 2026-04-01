-- part of a query repo
-- query name: Transaction Fees on Arbitrum
-- query link: https://dune.com/queries/3223375


WITH balancer_contracts AS (
        SELECT 0xBA12222222228d8Ba445958a75a0704d566BF2C8 AS contract_address
        
        UNION ALL

        SELECT poolAddress AS contract_address
        FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered
        
        UNION ALL
        
        SELECT gauge AS contract_address 
        FROM balancer_arbitrum.ChildChainGaugeFactory_evt_GaugeCreated
        
        UNION ALL 
        
        SELECT gauge AS contract_address
        FROM balancer_arbitrum.ChildChainLiquidityGaugeFactory_evt_RewardsOnlyGaugeCreated
    ),
    
    balancer_transactions AS (
        SELECT DISTINCT tx_hash
        FROM balancer_contracts b
        JOIN arbitrum.logs l
        ON b.contract_address = l.contract_address
        AND block_time <= TIMESTAMP '{{End date}}'
        AND block_time >= TIMESTAMP '{{Start date}}'
    )

SELECT SUM(CAST(gas_used AS INT256) * CAST(effective_gas_price AS INT256)) / POW(10, 18) AS transaction_fee
FROM balancer_transactions b 
JOIN arbitrum.transactions t
ON t.hash = b.tx_hash
