-- part of a query repo
-- query name: Protocol Fees by Blockchain
-- query link: https://dune.com/queries/3266768


WITH pool_labels AS(
    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v2_pools
)

SELECT 
    CASE WHEN '{{Aggregation}}' = 'Monthly'
    THEN DATE_TRUNC('month', day) 
    WHEN '{{Aggregation}}' = 'Weekly'
    THEN DATE_TRUNC('week', day)
    WHEN '{{Aggregation}}' = 'Daily'
    THEN DATE_TRUNC('day', day)
    END AS _date, 
      f.blockchain || 
        CASE 
            WHEN f.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN f.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN f.blockchain = 'base' THEN ' 🟨'
            WHEN f.blockchain = 'ethereum' THEN ' Ξ'
            WHEN f.blockchain = 'gnosis' THEN ' 🟩'
            WHEN f.blockchain = 'optimism' THEN ' 🔴'
            WHEN f.blockchain = 'polygon' THEN ' 🟪'
            WHEN f.blockchain = 'zkevm' THEN ' 🟣'
        END 
    AS blockchain,  
SUM(protocol_fee_collected_usd) as protocol,
SUM(treasury_fee_usd) as treasury
FROM balancer.protocol_fee f 
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = f.blockchain AND c.pool = f.pool_id
LEFT JOIN pool_labels l 
ON l.blockchain = f.blockchain AND l.address = f.pool_address
WHERE day >= TIMESTAMP '{{Start Date}}'
AND ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
GROUP BY 1, 2