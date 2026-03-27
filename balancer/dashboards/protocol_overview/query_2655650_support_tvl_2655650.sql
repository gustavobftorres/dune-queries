-- part of a query repo
-- query name: (query_2655650) support_tvl
-- query link: https://dune.com/queries/2655650


/*
queried on:
Balancer Analysis by Blockchain https://dune.com/queries/2617531
*/
WITH 
    all_tvl AS ( 
        SELECT 
            blockchain, 
            SUM(protocol_liquidity_usd) AS tvl
        FROM balancer.liquidity
        WHERE day = CURRENT_DATE
        GROUP BY 1
    ), 
    
    tvl AS (
        SELECT 
            a.blockchain, 
            a.tvl, 
            (SELECT SUM(tvl) FROM all_tvl) AS total_tvl,
            a.tvl / (SELECT SUM(tvl) FROM all_tvl) AS percentage_tvl,
            (SELECT SUM(tvl) FROM all_tvl)/1e9 as short_tvl
        FROM all_tvl a

    )
SELECT * FROM tvl