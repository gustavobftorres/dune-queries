-- part of a query repo
-- query name: Balancer CowSwap AMM Weekly Volume by Blockchain
-- query link: https://dune.com/queries/3954410


/* Volume per week */
/* Visualization: bar chart */

    SELECT
        DATE_TRUNC('week', block_time) AS week,
             s.blockchain || 
        CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩'
            WHEN s.blockchain = 'base' THEN ' 🟨'
        END 
    AS blockchain,
        SUM(amount_usd) AS volume
    FROM
        balancer_cowswap_amm.trades s
        WHERE block_time >=   TIMESTAMP '{{1. Start date}}' 
        AND ('{{2. Blockchain}}' = 'All' OR blockchain = '{{2. Blockchain}}')
    GROUP BY 1, 2