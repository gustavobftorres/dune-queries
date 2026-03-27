-- part of a query repo
-- query name: V3 Swap Gas Costs
-- query link: https://dune.com/queries/4644217


WITH eth_transactions AS(
SELECT 
    'ethereum' AS blockchain,
    tx.block_time, 
    project_contract_address, 
    pool_symbol,
    pool_type,
    tx_hash,
    tx.gas_used
FROM ethereum.transactions tx
JOIN balancer.trades s ON s.tx_hash = tx.hash
AND s.blockchain = 'ethereum'
WHERE s.project = 'balancer'
AND s.version = '3'
AND tx.block_time >= NOW() - interval '5' day
ORDER BY 6 DESC),

gnosis_transactions AS(
SELECT 
    'gnosis' AS blockchain,
    tx.block_time, 
    project_contract_address, 
    pool_symbol,
    pool_type,
    tx_hash,
    tx.gas_used
FROM gnosis.transactions tx
JOIN balancer.trades s ON s.tx_hash = tx.hash
AND s.blockchain = 'gnosis'
WHERE s.project = 'balancer'
AND s.version = '3'
AND tx.block_time >= NOW() - interval '5' day
)

SELECT * FROM eth_transactions
UNION
SELECT * FROM gnosis_transactions
ORDER BY 7 ASC