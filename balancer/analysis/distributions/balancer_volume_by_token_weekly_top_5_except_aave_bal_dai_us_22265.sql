-- part of a query repo
-- query name: Balancer Volume By Token (weekly top 5 except AAVE, BAL, DAI, USDC, WBTC & WETH) (Dune SQL)
-- query link: https://dune.com/queries/22265


WITH swaps AS (
        SELECT
            date_trunc('week', d.block_time) AS week,
            sum(amount_usd) AS volume,
            CAST(d.token_bought_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST(t.contract_address as varchar) = CAST (d.token_bought_address as varchar)
        WHERE project = 'balancer'
                AND d.token_bought_address NOT IN (0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9,
                    0xba100000625a3754423978a60c9317c58a424e3d,
                    0x6b175474e89094c44da98b954eedeac495271d0f,
                    0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        GROUP BY 1,3,4
        
        UNION ALL
        
        SELECT
            date_trunc('week', d.block_time) AS week,
            sum(amount_usd) AS volume,
            CAST(d.token_sold_address as varchar) AS address,
            t.symbol AS token
        FROM dex.trades d
        LEFT JOIN tokens.erc20 t ON CAST(t.contract_address as varchar) = CAST (d.token_bought_address as varchar)
        WHERE project = 'balancer'
                AND d.token_sold_address NOT IN (0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9,
                    0xba100000625a3754423978a60c9317c58a424e3d,
                    0x6b175474e89094c44da98b954eedeac495271d0f,
                    0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48,
                    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2,
                    0x2260fac5e5542a773aa44fbcfedf7c193bc2c599)
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        GROUP BY 1,3,4
)

SELECT * FROM (
        SELECT
        s.week,
        COALESCE(s.token, CONCAT(SUBSTRING(s.address, 3, 6), '...')) AS token,
        s.address,
        ROW_NUMBER() OVER (PARTITION BY week ORDER BY SUM(volume) DESC NULLS LAST) AS position,
        sum(s.volume)/2 AS volume
    FROM swaps s
    GROUP BY 1, 2, 3
    ORDER BY 1, 3
) ranking
WHERE position <= 5