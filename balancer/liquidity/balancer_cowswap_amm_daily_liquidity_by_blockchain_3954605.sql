-- part of a query repo
-- query name: Balancer CoWSwap AMM Daily Liquidity by Blockchain
-- query link: https://dune.com/queries/3954605


WITH

liquidity AS (
    SELECT 
        day,
        blockchain,
        CASE WHEN '{{3. TVL Currency}}' = 'USD'
        THEN SUM(protocol_liquidity_usd) 
        WHEN '{{3. TVL Currency}}' = 'eth'
        THEN SUM(protocol_liquidity_eth) 
        END AS tvl
    FROM balancer_cowswap_amm.liquidity t
    WHERE day >= TIMESTAMP '{{1. Start date}}' 
    AND ('{{2. Blockchain}}' = 'All' OR t.blockchain = '{{2. Blockchain}}')
    GROUP BY 1, 2
    )

    SELECT
        day AS week,
             s.blockchain || 
        CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩'
            WHEN s.blockchain = 'base' THEN ' 🟨'
        END 
    AS blockchain,
        tvl AS median_liquidity
    FROM liquidity s
    GROUP BY 1, 2, 3
