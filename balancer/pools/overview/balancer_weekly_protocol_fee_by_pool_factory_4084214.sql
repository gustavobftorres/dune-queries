-- part of a query repo
-- query name: Balancer Weekly Protocol Fee by Pool Factory
-- query link: https://dune.com/queries/4084214


   SELECT
        DATE_TRUNC('week', day) AS week,
        factory_version,
        SUM(protocol_fee_collected_usd) AS fees
    FROM balancer.protocol_fee t
    INNER JOIN query_4080393 q ON t.pool_address = q.pool_address
    AND t.blockchain = q.blockchain
    WHERE day >=   TIMESTAMP '{{1. Start date}}' 
    AND ('{{2. Pool Factory}}' = 'All' OR t.blockchain = '{{2. Pool Factory}}')   
    AND ('{{3. Blockchain}}' = 'All' OR q.factory_version = '{{3. Blockchain}}')
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
