-- part of a query repo
-- query name: Balancer Daily Traders on Polygon
-- query link: https://dune.com/queries/95315


SELECT 
    date_trunc('day', call_block_time) AS day,
    COUNT(DISTINCT (funds->>'sender')) AS traders
FROM balancer_v2."Vault_call_swap"
WHERE call_success
AND call_block_time >= '{{2. Start date}}'
AND call_block_time <= '{{3. End date}}'
GROUP BY 1