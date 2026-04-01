-- part of a query repo
-- query name: Balancer TVL by Network
-- query link: https://dune.com/queries/3119405


WITH 
    chain_tvl_data AS (
        SELECT 
            blockchain || ' ' || ' ' ||  
                CASE 
                    WHEN blockchain = 'arbitrum' THEN '| 🟦'
                    WHEN blockchain = 'avalanche_c' THEN '| ⬜ '
                    WHEN blockchain = 'base' THEN '| 🟨'
                    WHEN blockchain = 'ethereum' THEN ' | Ξ'
                    WHEN blockchain = 'gnosis' THEN '| 🟩'
                    WHEN blockchain = 'optimism' THEN '| 🔴'
                    WHEN blockchain = 'polygon' THEN '| 🟪'
                    WHEN blockchain = 'zkevm' THEN '| 🟣'
                END AS blockchain,
            CAST(day AS TIMESTAMP) AS day,
            CASE WHEN '{{Currency}}' = 'USD'
                THEN SUM(protocol_liquidity_usd)
                WHEN '{{Currency}}' = 'eth'
                THEN SUM(protocol_liquidity_eth)
            END AS chain_tvl,
            CASE WHEN '{{Currency}}' = 'USD'
                THEN SUM(protocol_liquidity_usd) FILTER(WHERE x.day = y.latest_day) 
                WHEN '{{Currency}}' = 'eth'
                THEN SUM(protocol_liquidity_eth) FILTER(WHERE x.day = y.latest_day) 
            END AS latest_tvl,
            MAX(latest_day) AS latest_day
        FROM balancer.liquidity x
        LEFT JOIN (SELECT MAX(day) AS latest_day FROM balancer.liquidity WHERE version = '2') y
            ON y.latest_day = x.day
        WHERE day >= current_date - INTERVAL '{{Date Range in Days}}' DAY
            AND day <= (SELECT MAX(day) FROM balancer.liquidity WHERE version = '2')
        GROUP BY 1, 2
    )

SELECT * FROM chain_tvl_data
ORDER BY day DESC, chain_tvl DESC;
