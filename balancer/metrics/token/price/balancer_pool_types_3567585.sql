-- part of a query repo
-- query name: Balancer Pool Types
-- query link: https://dune.com/queries/3567585


SELECT DISTINCT pool_type FROM labels.balancer_v2_pools
UNION ALL
SELECT 'All'
ORDER BY 1 ASC