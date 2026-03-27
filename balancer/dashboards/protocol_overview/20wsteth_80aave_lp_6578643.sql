-- part of a query repo
-- query name: 20wstETH-80AAVE LP
-- query link: https://dune.com/queries/6578643


-- Query: LPs of ABPT pool via Aave Safety Module (stkABPT)
WITH 
-- All transfers of stkABPT (Aave staked ABPT token)
stk_abpt_transfers AS (
    SELECT 
        "to" as address,
        CAST(value AS DOUBLE) / 1e18 as amount
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x9eDA81C21C273a82BE9Bbc19B6A6182212068101 -- stkABPT
    AND "to" != 0x0000000000000000000000000000000000000000
    
    UNION ALL
    
    SELECT 
        "from" as address,
        -CAST(value AS DOUBLE) / 1e18 as amount
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x9eDA81C21C273a82BE9Bbc19B6A6182212068101
    AND "from" != 0x0000000000000000000000000000000000000000
),
-- Current stkABPT balances
stk_abpt_balances AS (
    SELECT 
        address as user_address,
        SUM(amount) as balance,
        'aave_staked' as source
    FROM stk_abpt_transfers
    WHERE address != 0x9eDA81C21C273a82BE9Bbc19B6A6182212068101 -- Exclude the staking contract itself
    GROUP BY address
    HAVING SUM(amount) > 0.0001
),
-- Direct ABPT holders (not staked in Aave)
abpt_transfers AS (
    SELECT 
        "to" as address,
        CAST(value AS DOUBLE) / 1e18 as amount
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x3de27efa2f1aa663ae5d458857e731c129069f29 -- ABPT
    AND "to" NOT IN (
        0x0000000000000000000000000000000000000000,
        0xBA12222222228d8Ba445958a75a0704d566BF2C8, -- Balancer Vault
        0x9eDA81C21C273a82BE9Bbc19B6A6182212068101  -- stkABPT contract
    )
    
    UNION ALL
    
    SELECT 
        "from" as address,
        -CAST(value AS DOUBLE) / 1e18 as amount
    FROM erc20_ethereum.evt_Transfer
    WHERE contract_address = 0x3de27efa2f1aa663ae5d458857e731c129069f29
    AND "from" NOT IN (
        0x0000000000000000000000000000000000000000,
        0xBA12222222228d8Ba445958a75a0704d566BF2C8,
        0x9eDA81C21C273a82BE9Bbc19B6A6182212068101
    )
),
abpt_balances AS (
    SELECT 
        address as user_address,
        SUM(amount) as balance,
        'direct_wallet' as source
    FROM abpt_transfers
    GROUP BY address
    HAVING SUM(amount) > 0.0001
),
-- Combine all
all_lps AS (
    SELECT * FROM stk_abpt_balances
    UNION ALL
    SELECT * FROM abpt_balances
),
total_tvl AS (
    SELECT SUM(balance) as total FROM all_lps
)
SELECT 
    user_address,
    source,
    balance as bpt_balance,
    balance / (SELECT total FROM total_tvl) * 100 as percentage_of_pool,
    CASE 
        WHEN balance / (SELECT total FROM total_tvl) >= 0.01 THEN '🐋 Whale (>1%)'
        WHEN balance / (SELECT total FROM total_tvl) >= 0.001 THEN '🦈 Large (>0.1%)'
        ELSE '🐟 Small'
    END as lp_size
FROM all_lps
ORDER BY balance DESC
LIMIT 100;