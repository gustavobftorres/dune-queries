-- part of a query repo
-- query name: Protocol Fees Overview by Blockchain
-- query link: https://dune.com/queries/3266879


WITH pool_labels AS(
    SELECT
        blockchain,
        address,
        pool_type
    FROM labels.balancer_v2_pools
)

SELECT 
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
    SUM(protocol_fee_collected_usd) AS all_time_protocol,
    SUM(CASE WHEN day >= current_date - interval '30' day THEN protocol_fee_collected_usd ELSE 0 END) AS "30_day_protocol",
    SUM(CASE WHEN day >= current_date - interval '7' day THEN protocol_fee_collected_usd ELSE 0 END) AS "7_day_protocol",
    SUM(treasury_fee_usd) AS all_time_treasury,
    SUM(CASE WHEN day >= current_date - interval '30' day THEN treasury_fee_usd ELSE 0 END) AS "30_day_treasury",
    SUM(CASE WHEN day >= current_date - interval '7' day THEN treasury_fee_usd ELSE 0 END) AS "7_day_treasury"
FROM balancer.protocol_fee f
LEFT JOIN dune.balancer.dataset_core_pools c 
ON c.network = f.blockchain AND c.pool = f.pool_id
LEFT JOIN pool_labels l 
ON l.blockchain = f.blockchain AND l.address = f.pool_address
WHERE ('{{Only Core Pools}}' = 'No' OR c.network IS NOT NULL)
AND ('{{Pool Type}}' = 'All' OR l.pool_type = '{{Pool Type}}')
AND protocol_fee_collected_usd < 1000000000
GROUP BY f.blockchain
ORDER BY 1 ASC;