-- part of a query repo
-- query name: CoW volume on arbitrum by token pair
-- query link: https://dune.com/queries/3908663


SELECT token_pair, sum(amount_usd) AS volume FROM dex.trades
WHERE tx_to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41
AND blockchain = 'arbitrum'
GROUP BY 1
ORDER BY 2 DESc