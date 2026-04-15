-- part of a query repo
-- query name: chain_coverage
-- query link: https://dune.com/queries/6952113



SELECT blockchain, COUNT(*) as cnt
FROM evms.contracts
WHERE blockchain IN ('kaia', 'opbnb', 'celo', 'mantle', 'blast', 'scroll', 'sonic', 'gnosis', 'linea', 'zksync')
GROUP BY 1
ORDER BY cnt DESC
