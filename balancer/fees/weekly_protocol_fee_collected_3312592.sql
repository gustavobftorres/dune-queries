-- part of a query repo
-- query name: Weekly Protocol Fee Collected
-- query link: https://dune.com/queries/3312592


    SELECT
        DATE_TRUNC('week', day) AS week,
             s.blockchain || 
        CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN s.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN s.blockchain = 'base' THEN ' 🟨'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩'
            WHEN s.blockchain = 'optimism' THEN ' 🔴'
            WHEN s.blockchain = 'polygon' THEN ' 🟪'
            WHEN s.blockchain = 'zkevm' THEN ' 🟣'
        END 
        AS blockchain,
        SUM(protocol_fee_collected_usd) AS volume
    FROM
        balancer.protocol_fee s
        WHERE day >=   TIMESTAMP '{{1. Start date}}' 
        AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
        AND protocol_fee_collected_usd < 1000000000
    GROUP BY 1, 2
    ORDER BY 1 DESC