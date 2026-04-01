-- part of a query repo
-- query name: Pool Weekly Volume
-- query link: https://dune.com/queries/2707229


WITH swaps as(
SELECT  
    date_trunc('week', block_date) as week, 
    CAST(project_contract_address as varchar) as pool_id, 
    sum(amount_usd) as volume
FROM balancer.trades
WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
GROUP BY 1, 2)

SELECT 
    *
FROM swaps
WHERE week <= TIMESTAMP '{{3. End date}}'
AND week >= TIMESTAMP '{{2. Start date}}'
AND ('{{1. Pool ID}}' = 'All' OR pool_id = SUBSTRING('{{1. Pool ID}}',1,42))