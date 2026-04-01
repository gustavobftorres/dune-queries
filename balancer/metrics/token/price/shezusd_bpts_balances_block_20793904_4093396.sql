-- part of a query repo
-- query name: shezUSD BPTs Balances @ Block 20793904
-- query link: https://dune.com/queries/4093396


WITH transfers AS (
    SELECT 
        "from" AS wallet_address,
        -value AS amount,
        CASE 
            WHEN contract_address = 0xEd0DF9Cd16D806E8A523805e53cf0c56E6dB4D1d THEN 'wallet_balance'
            WHEN contract_address = 0xa71adac76a2e34f8f988fa6992e3bdad08d92c01 THEN 'gauge_balance'
            WHEN contract_address = 0x5153dd9b05ac642e387c87a12c89e97fe1be6860 THEN 'aura_balance'
            WHEN contract_address = 0xD7475b5941536ea4236e923997107BAaDa1Fb5E7 THEN 'beefy_balance'
        END AS balance_type
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address IN (
        0xEd0DF9Cd16D806E8A523805e53cf0c56E6dB4D1d,
        0xa71adac76a2e34f8f988fa6992e3bdad08d92c01,
        0x5153dd9b05ac642e387c87a12c89e97fe1be6860,
        0xD7475b5941536ea4236e923997107BAaDa1Fb5E7
    )
    AND evt_block_number < {{block no}}

    UNION ALL

    SELECT 
        to AS wallet_address,
        value AS amount,
        CASE 
            WHEN contract_address = 0xEd0DF9Cd16D806E8A523805e53cf0c56E6dB4D1d THEN 'wallet_balance'
            WHEN contract_address = 0xa71adac76a2e34f8f988fa6992e3bdad08d92c01 THEN 'gauge_balance'
            WHEN contract_address = 0x5153dd9b05ac642e387c87a12c89e97fe1be6860 THEN 'aura_balance'
            WHEN contract_address = 0xD7475b5941536ea4236e923997107BAaDa1Fb5E7 THEN 'beefy_balance'
        END AS balance_type
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address IN (
        0xEd0DF9Cd16D806E8A523805e53cf0c56E6dB4D1d,
        0xa71adac76a2e34f8f988fa6992e3bdad08d92c01,
        0x5153dd9b05ac642e387c87a12c89e97fe1be6860,
        0xD7475b5941536ea4236e923997107BAaDa1Fb5E7
    )
     AND evt_block_number < {{block no}}
),

    balances AS (
        SELECT 
            wallet_address,
            SUM(CASE WHEN balance_type = 'wallet_balance' THEN amount ELSE 0 END) AS wallet_balance,
            SUM(CASE WHEN balance_type = 'gauge_balance' THEN amount ELSE 0 END) AS gauge_balance,
            SUM(CASE WHEN balance_type = 'aura_balance' THEN amount ELSE 0 END) AS aura_balance,
            SUM(CASE WHEN balance_type = 'beefy_balance' THEN amount ELSE 0 END) AS beefy_balance
        FROM transfers
        WHERE wallet_address NOT IN (
            0xBA12222222228d8Ba445958a75a0704d566BF2C8, -- Vault
            0xa71adac76a2e34f8f988fa6992e3bdad08d92c01, -- Balancer Gauge
            0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2, -- Aura VoterProxy
            0x0000000000000000000000000000000000000000,  -- Zero Address
            0x20b189339ffa972c5f0908eabca57fbec96a1c64 -- Beefy Strategy
        )
        GROUP BY wallet_address    
    ),

final AS (SELECT
    wallet_address,
    wallet_balance / 1e18 AS wallet_balance,
    gauge_balance / 1e18 AS gauge_balance,
    aura_balance / 1e18 AS aura_balance,
    beefy_balance / 1e18 AS beefy_balance
FROM balances)

SELECT
    wallet_address,
    wallet_balance,
    gauge_balance,
    aura_balance,
    beefy_balance,
    wallet_balance + gauge_balance + aura_balance + beefy_balance AS total_balance
FROM final
WHERE (wallet_balance > 0 OR gauge_balance > 0 OR aura_balance> 0 OR beefy_balance > 0)
GROUP BY 1, 2, 3, 4, 5
ORDER BY 6 DESC