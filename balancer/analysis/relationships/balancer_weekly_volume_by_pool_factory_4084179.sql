-- part of a query repo
-- query name: Balancer Weekly Volume by Pool Factory
-- query link: https://dune.com/queries/4084179


   SELECT
        DATE_TRUNC('week', block_time) AS week,
        factory_version,
        SUM(amount_usd) AS volume
    FROM balancer.trades t
    INNER JOIN query_4080393 q ON t.project_contract_address = q.pool_address
    AND t.blockchain = q.blockchain
    WHERE block_time >=   TIMESTAMP '{{1. Start date}}' 
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND ('{{2. Pool Factory}}' = 'All' OR q.factory_version = '{{2. Pool Factory}}')   
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
