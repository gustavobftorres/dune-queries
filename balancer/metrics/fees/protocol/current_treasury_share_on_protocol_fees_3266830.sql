-- part of a query repo
-- query name: Current Treasury Share on Protocol Fees
-- query link: https://dune.com/queries/3266830


SELECT treasury_share * 100
FROM balancer.protocol_fee
WHERE day = CURRENT_DATE
LIMIT 1