-- part of a query repo
-- query name: Fee comparison
-- query link: https://dune.com/queries/3629395


SELECT 
    day,
    'balancer' AS project,
    SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    AND ('{{Balancer Pool Type}}' = 'All' OR pool_type = '{{Balancer Pool Type}}')
    GROUP BY 1,2

UNION ALL

SELECT
    day,
    'aerodrome' AS project,
    SUM(total_fees_usd) AS fees
    FROM query_3629064
    WHERE day <= (SELECT MAX(day) FROM balancer.protocol_fee)
    AND day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Aero/Velo Pool Type}}' = 'All' OR pool_type = '{{Aero/Velo Pool Type}}')
    GROUP BY 1,2
    
UNION ALL

SELECT
    day,
    'velodrome' AS project,
    SUM(total_fees_usd) AS fees
    FROM query_3629024
    WHERE day <= (SELECT MAX(day) FROM balancer.protocol_fee)
    AND day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Aero/Velo Pool Type}}' = 'All' OR pool_type = '{{Aero/Velo Pool Type}}')
    GROUP BY 1,2