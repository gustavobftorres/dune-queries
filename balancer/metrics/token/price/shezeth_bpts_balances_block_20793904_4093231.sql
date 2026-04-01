-- part of a query repo
-- query name: shezETH BPTs Balances @ Block 20793904
-- query link: https://dune.com/queries/4093231


WITH transfers AS (
    SELECT 
        "from" AS wallet_address,
        -value AS amount,
        CASE 
            WHEN contract_address = 0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382 THEN 'wallet_balance'
            WHEN contract_address = 0xB002073210698B0852E34DA6A5e432a04D299205 THEN 'gauge_balance'
            WHEN contract_address = 0x747eF4e13cB71264897af2D69855f56b771b42ce THEN 'aura_balance'
            WHEN contract_address = 0x0258B57559e4EC229e3a710D2a55447eaea2312D THEN 'beefy_balance'
        END AS balance_type
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address IN (
        0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382,
        0xB002073210698B0852E34DA6A5e432a04D299205,
        0x747eF4e13cB71264897af2D69855f56b771b42ce,
        0x0258B57559e4EC229e3a710D2a55447eaea2312D
    )
    AND evt_block_number < {{block no}}

    UNION ALL

    SELECT 
        to AS wallet_address,
        value AS amount,
        CASE 
            WHEN contract_address = 0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382 THEN 'wallet_balance'
            WHEN contract_address = 0xB002073210698B0852E34DA6A5e432a04D299205 THEN 'gauge_balance'
            WHEN contract_address = 0x747eF4e13cB71264897af2D69855f56b771b42ce THEN 'aura_balance'
            WHEN contract_address = 0x0258B57559e4EC229e3a710D2a55447eaea2312D THEN 'beefy_balance'
        END AS balance_type
    FROM erc20_ethereum.evt_transfer
    WHERE contract_address IN (
        0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382,
        0xB002073210698B0852E34DA6A5e432a04D299205,
        0x747eF4e13cB71264897af2D69855f56b771b42ce,
        0x0258B57559e4EC229e3a710D2a55447eaea2312D
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
            0xB002073210698B0852E34DA6A5e432a04D299205, -- Balancer Gauge
            0xaf52695e1bb01a16d33d7194c28c42b10e0dbec2, -- Aura VoterProxy
            0x0000000000000000000000000000000000000000, -- Zero Address
            0xf5404B321A3Ea2D41999147856E1c66910bDD2Da -- Beefy Strategy
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