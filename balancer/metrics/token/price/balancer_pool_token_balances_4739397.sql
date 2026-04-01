-- part of a query repo
-- query name: Balancer Pool Token Balances
-- query link: https://dune.com/queries/4739397



WITH prices AS(
SELECT
    DATE_TRUNC('hour', minute) AS hour,
    contract_address,
    APPROX_PERCENTILE(price,0.5) AS median_price
FROM prices.usd
WHERE blockchain = '{{blockchain}}'
AND contract_address IN ({{token 1}}, {{token 2}})
GROUP BY 1, 2 
),

calc AS(
SELECT DISTINCT
    DATE_TRUNC('hour', evt_block_time) AS hour,
    token_address,
    token_symbol,
    SUM(delta_amount) OVER (PARTITION BY token_symbol ORDER BY DATE_TRUNC('hour', evt_block_time)) AS balance,
    SUM(delta_amount * median_price) OVER (PARTITION BY token_symbol ORDER BY DATE_TRUNC('hour', evt_block_time)) AS balance_usd
FROM balancer.token_balance_changes t
JOIN prices p ON DATE_TRUNC('hour', evt_block_time) = p.hour
AND p.contract_address = t.token_address
WHERE (blockchain = '{{blockchain}}')
AND pool_address = {{balancer pool}})

SELECT 
    *,
    CASE WHEN token_address = {{token 1}} THEN balance END AS token_1_balance,
    CASE WHEN token_address = {{token 2}} THEN balance END AS token_2_balance
FROM calc
WHERE balance > 0
ORDER BY 1 DESC