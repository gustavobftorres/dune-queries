-- part of a query repo
-- query name: Labbeling
-- query link: https://dune.com/queries/6768837


WITH channels AS (
    SELECT channel, blockchain, SUM(volume) AS volume
    FROM query_6768760
    GROUP BY 1, 2
),
labels AS (
    SELECT CAST(address as varchar) AS address, blockchain, name
    FROM query_3004790
)
SELECT s.blockchain, l.name, SUM(s.volume) AS volume
FROM labels l
LEFT JOIN channels s ON l.address = s.channel AND l.blockchain = s.blockchain
GROUP BY 1, 2