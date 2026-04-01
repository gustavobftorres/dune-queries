-- part of a query repo
-- query name: Balancer CoWSwap AMM Pool Weekly Volume
-- query link: https://dune.com/queries/3965056


WITH swaps as(
SELECT  
    date_trunc('week', block_date) as week, 
    project_contract_address as pool_id, 
    sum(amount_usd) as volume
FROM balancer_cowswap_amm.trades
WHERE ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
GROUP BY 1, 2)

SELECT 
    *
FROM swaps
WHERE week <= TIMESTAMP '{{3. End date}}'
AND week >= TIMESTAMP '{{2. Start date}}'
AND ('{{1. Pool Address}}' = 'All' OR pool_id = {{1. Pool Address}})