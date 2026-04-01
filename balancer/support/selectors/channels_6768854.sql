-- part of a query repo
-- query name: Channels
-- query link: https://dune.com/queries/6768854


SELECT channel, blockchain, SUM(volume) AS volume
FROM query_6768760
GROUP BY 1, 2