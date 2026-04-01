-- part of a query repo
-- query name: LSTs Volume by Token Pair on Optimism
-- query link: https://dune.com/queries/3921285


WITH lst_pools AS(
SELECT * FROM dune.balancer.result_lst_pools),

trades AS (
SELECT 
    CAST(DATE_TRUNC('day', block_date) AS TIMESTAMP) AS date, 
        token_pair, 
        SUM(amount_usd) AS amount_usd
FROM balancer.trades t
INNER JOIN lst_pools l ON l.pool_address = t.project_contract_address
                        AND l.blockchain = t.blockchain
WHERE t.block_date >= TIMESTAMP '{{Start date}}'
AND t.block_date <= TIMESTAMP '{{End date}}'
AND t.blockchain = 'optimism'
GROUP BY 1, 2
HAVING sum(amount_usd) > 1000
)

SELECT * FROM trades