-- part of a query repo
-- query name: balancer_contracts_submitted
-- query link: https://dune.com/queries/2714287


SELECT address, namespace,name, created_at, 'ethereum' as blockchain FROM ethereum.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'arbitrum' as blockchain FROM arbitrum.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'avalanche_c' as blockchain FROM avalanche_c.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'gnosis' as blockchain FROM gnosis.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'optimism' as blockchain FROM optimism.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'polygon' as blockchain FROM polygon.contracts_submitted WHERE namespace = 'balancer_v2'
UNION ALL
SELECT address, namespace,name, created_at, 'base' as blockchain FROM base.contracts_submitted WHERE namespace = 'balancer_v2'