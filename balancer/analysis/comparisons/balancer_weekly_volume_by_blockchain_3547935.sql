-- part of a query repo
-- query name: Balancer Weekly Volume by Blockchain
-- query link: https://dune.com/queries/3547935


/* Volume per week */
/* Visualization: bar chart */

    SELECT
        DATE_TRUNC('week', block_time) AS week,
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
        SUM(amount_usd) AS volume
    FROM
        balancer.trades s
        WHERE block_time >=   TIMESTAMP '{{1. Start date}}' 
        AND ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
        AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    GROUP BY 1, 2