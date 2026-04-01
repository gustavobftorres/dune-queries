-- part of a query repo
-- query name: Balancer Traders on Polygon
-- query link: https://dune.com/queries/95202


SELECT COUNT(DISTINCT (funds->>'sender')) AS traders
FROM balancer_v2."Vault_call_swap" s
WHERE call_success