-- part of a query repo
-- query name: Balancer V3 Volume by Pool
-- query link: https://dune.com/queries/4560669



SELECT
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS week, 
        CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦'
            WHEN s.blockchain = 'avalanche_c' THEN ' ⬜ '
            WHEN s.blockchain = 'base' THEN ' 🟨'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩'
            WHEN s.blockchain = 'optimism' THEN ' 🔴'
            WHEN s.blockchain = 'polygon' THEN ' 🟪'
            WHEN s.blockchain = 'zkevm' THEN ' 🟣'
        END || s.pool_symbol
    AS pool,
        SUM(amount_usd) AS volume
    FROM
        balancer.trades s
        WHERE 1 = 1
        AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
        AND version = '3'
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC