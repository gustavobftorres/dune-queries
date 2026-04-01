-- part of a query repo
-- query name: TVL by DEX
-- query link: https://dune.com/queries/190412


WITH eth_price AS (
        SELECT
            date_trunc('day', minute) AS day,
            AVG(price) AS price
        FROM prices.usd
        WHERE contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        GROUP BY 1
    ),
    
    dex_prices_1 AS (
        SELECT 
            date_trunc('day', hour) AS day,
            contract_address AS token,
            (PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY median_price)) AS price,
            SUM(sample_size) as sample_size
        FROM dex.view_token_prices
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
        AND AVG(median_price) < 1e6
    ),
    
    dex_prices AS (
        SELECT *, LEAD(day, 1, now()) OVER (PARTITION BY token ORDER BY day) AS day_of_next_change
        FROM dex_prices_1
    ),
    
    curve_eth AS (
        SELECT t.day, price*amount AS liquidity FROM (
        SELECT t.day, SUM(SUM(amount)/1e18) OVER (ORDER BY t.day)  as amount
            FROM (
                SELECT date_trunc('day', block_time) AS day, -tr.value AS amount
                FROM ethereum.traces tr
                WHERE "from" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
            
                UNION ALL
                
                SELECT date_trunc('day', block_time) AS day, value AS amount
                FROM ethereum.traces
                WHERE "to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
            ) t
            GROUP BY 1
            ) t
            LEFT JOIN eth_price p
            ON t.day = p.day 
    ),
    
    liquidity_estimates as (
        select 
            l.day,
            project,
            pool_address,
            SUM(token_usd_amount)/COALESCE(SUM(token_pool_percentage), 1) AS liquidity
        from dex.liquidity l
        where project IN ('Balancer', 'Uniswap', 'Sushiswap', 'Curve')
        group by 1, 2, 3
        
        union all
        
        select day, 'Curve' AS project, NULL::bytea AS pool_address, liquidity
        from curve_eth
    ),
    
    tvl_by_project AS (
        select day, project, sum(liquidity) as tvl
        from liquidity_estimates
        group by 1, 2
    )

select * from tvl_by_project
WHERE day >= '{{2. Start date}}'
AND day <= '{{3. End date}}'