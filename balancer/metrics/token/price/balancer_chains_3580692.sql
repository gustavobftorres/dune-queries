-- part of a query repo
-- query name: Balancer chains
-- query link: https://dune.com/queries/3580692


SELECT DISTINCT blockchain AS blockchain 
FROM balancer.liquidity
UNION 
SELECT 'All' AS blockchain
ORDER BY 1 ASC