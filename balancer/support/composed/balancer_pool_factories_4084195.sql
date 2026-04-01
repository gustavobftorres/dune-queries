-- part of a query repo
-- query name: Balancer Pool Factories
-- query link: https://dune.com/queries/4084195


SELECT DISTINCT factory_version
FROM query_4080393
UNION ALL 
SELECT 'All'
ORDER BY 1 ASC