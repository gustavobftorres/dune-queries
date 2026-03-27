-- part of a query repo
-- query name: ECLPs / Slipstream Fee comparison
-- query link: https://dune.com/queries/3630260


SELECT 
    day,
    'balancer' AS project,
    SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND pool_type = 'ECLP'
    GROUP BY 1,2
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    SUM(fees_usd) AS fees
    FROM query_3630153
    WHERE day <= (SELECT MAX(day) FROM balancer.protocol_fee)
    AND day >= TIMESTAMP '{{Start Date}}'
    GROUP BY 1,2