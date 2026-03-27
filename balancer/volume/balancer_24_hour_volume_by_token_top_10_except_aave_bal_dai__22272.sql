-- part of a query repo
-- query name: Balancer 24-hour Volume by Token (top 10 except AAVE, BAL, DAI, USDC, WBTC & WETH) (Dune SQL)
-- query link: https://dune.com/queries/22272


-- Volume (token breakdown) per hour (last 24 hours)
-- Visualization: bar chart (stacked)

WITH swaps AS (
        SELECT
            date_trunc('hour', block_time) AS hour,
            sum(amount_usd) AS volume,
            CAST (d.token_bought_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON t.contract_address = d.token_bought_address
        WHERE project = 'balancer'
        AND d.token_bought_address NOT IN (0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9,
                    0xba100000625a3754423978a60c9317c58a424e3d,
                    0x6b175474e89094c44da98b954eedeac495271d0f,
                    0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1' day)
        GROUP BY 1, 3, 4
        
        UNION ALL
        
        SELECT
            date_trunc('hour', block_time) AS hour,
           sum(amount_usd) AS volume,
           CAST(d.token_sold_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON t.contract_address = d.token_sold_address
        WHERE project = 'balancer'
        AND d.token_sold_address NOT IN (0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9,
                    0xba100000625a3754423978a60c9317c58a424e3d,
                    0x6b175474e89094c44da98b954eedeac495271d0f,
                    0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)
        AND date_trunc('hour', block_time) > date_trunc('hour', now() - interval '1' day)
        GROUP BY 1, 3, 4
),

    ranking AS (
        SELECT
            token,
            address,
            sum(volume)/2,
            ROW_NUMBER() OVER (ORDER BY sum(volume) DESC NULLS LAST) AS position
        FROM swaps
        GROUP BY 1, 2
)

SELECT
    s.hour,
    sum(s.volume)/2 AS volume,
    s.address,
    COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...')) AS token
FROM swaps s
LEFT JOIN ranking r ON r.address = s.address
WHERE r.position <= 10
GROUP BY 1,3,4