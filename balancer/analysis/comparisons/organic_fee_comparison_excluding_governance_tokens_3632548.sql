-- part of a query repo
-- query name: Organic Fee comparison - Excluding Governance Tokens
-- query link: https://dune.com/queries/3632548


SELECT 
    day,
    'balancer' AS project,
    SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee
    WHERE day >= TIMESTAMP '{{Start Date}}'
    AND ('{{Balancer Blockchain}}' = 'All' OR blockchain = '{{Balancer Blockchain}}')
    AND ('{{Balancer Pool Type}}' = 'All' OR pool_type = '{{Balancer Pool Type}}')
    AND upper(pool_symbol) NOT LIKE '%BAL%' --excluding governance token
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
    AND pair NOT LIKE '%AERO%' --excluding governance token
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
    AND pair NOT LIKE '%VELO%' --excluding governance token
    GROUP BY 1,2