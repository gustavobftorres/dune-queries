-- part of a query repo
-- query name: TVL comparison
-- query link: https://dune.com/queries/3626188


SELECT 
    day,
    'balancer' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(protocol_liquidity_eth)
    ELSE SUM(protocol_liquidity_usd)
    END AS liquidity
    FROM balancer.liquidity
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    AND ('{{Balancer Pool Type}}' = 'All' OR pool_type = '{{Balancer Pool Type}}')
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'aerodrome' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(liquidity_eth)
    ELSE SUM(liquidity_usd)
    END AS liquidity
    FROM query_3625769
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Aero/Velo Pool Type}}' = 'All' OR pool_type = '{{Aero/Velo Pool Type}}')
    GROUP BY 1,2
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    CASE WHEN '{{Currency}}' = 'eth'
    THEN SUM(liquidity_eth)
    ELSE SUM(liquidity_usd)
    END AS liquidity
    FROM query_3625994
    WHERE day <= (SELECT MAX(day) FROM balancer.liquidity)
    AND day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Aero/Velo Pool Type}}' = 'All' OR pool_type = '{{Aero/Velo Pool Type}}')
    GROUP BY 1,2