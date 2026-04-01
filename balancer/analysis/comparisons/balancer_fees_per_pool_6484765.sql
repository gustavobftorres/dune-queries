-- part of a query repo
-- query name: Balancer fees per pool
-- query link: https://dune.com/queries/6484765


SELECT * 
FROM balancer.protocol_fee
WHERE day >= TIMESTAMP '2025-01-01 00:00:00 UTC' 
AND token_amount_raw <> 0