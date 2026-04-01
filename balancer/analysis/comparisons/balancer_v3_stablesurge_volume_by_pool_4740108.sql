-- part of a query repo
-- query name: Balancer V3 StableSurge Volume by Pool
-- query link: https://dune.com/queries/4740108



SELECT
  DATE_TRUNC('week', block_date) AS week, 
       CASE 
            WHEN s.blockchain = 'arbitrum' THEN ' 🟦 |'
            WHEN s.blockchain = 'avalanche_c' THEN ' ⬜  |'
            WHEN s.blockchain = 'base' THEN ' 🟨 |'
            WHEN s.blockchain = 'ethereum' THEN ' Ξ |'
            WHEN s.blockchain = 'gnosis' THEN ' 🟩 |'
            WHEN s.blockchain = 'optimism' THEN ' 🔴 |'
            WHEN s.blockchain = 'polygon' THEN ' 🟪 |'
            WHEN s.blockchain = 'zkevm' THEN ' 🟣 |'
        END  || s.pool_symbol
    AS pool,
        SUM(COALESCE(amount_usd, 0)) AS volume
    FROM
        balancer.trades s
    INNER JOIN balancer_v3_multichain.stablesurgepoolfactory_call_create cc
        ON cc.chain = s.blockchain
            AND s.project_contract_address = cc.output_pool
    WHERE 1 = 1
        AND ('{{blockchain}}' = 'All' OR blockchain = '{{blockchain}}')
        AND version = '3'
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC