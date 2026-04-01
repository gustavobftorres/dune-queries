-- part of a query repo
-- query name: Current vlAURA Distribution per delegate
-- query link: https://dune.com/queries/4024403


SELECT
    delegate,
    sum(total_locked) AS total_locked,
    sum(voting_power) AS voting_power
FROM query_4023187
WHERE vlaura_round = (SELECT MAX(vlaura_round) FROM query_4023187)
GROUP BY 1
ORDER BY 2 DESC