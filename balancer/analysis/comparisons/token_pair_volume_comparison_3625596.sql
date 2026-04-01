-- part of a query repo
-- query name: Token Pair Volume Comparison
-- query link: https://dune.com/queries/3625596


SELECT block_date, project, sum(amount_usd) as volume
FROM dex.trades
WHERE token_pair = '{{Token Pair}}'
AND block_date >= TIMESTAMP '{{Start Date}}'
AND ('{{Blockchain}}' = 'All' OR blockchain = '{{Blockchain}}')
GROUP BY 1, 2