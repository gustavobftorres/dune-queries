-- part of a query repo
-- query name: 1inch / CowSwap Volume by DEX on Arbitrum
-- query link: https://dune.com/queries/3223316


SELECT date_trunc('week', block_date) as day, case when (project='balancer') then 'Balancer' else  'Others' end as project, sum(amount_usd) as volume
FROM dex.trades
WHERE 1=1
AND tx_to IN (0xad3b67bca8935cb510c8d18bd45f0b94f54a968f, 0x1111111254fb6c44bac0bed2854e76f90643097d, 0x1111111254eeb25477b68fb85ed929f73a960582, 0x9008d19f58aabd9ed0d60971565aa8510560ab41)
AND block_date <= TIMESTAMP '{{End date}}'
AND block_date >= TIMESTAMP '{{Start date}}' - INTERVAL '12' MONTH
AND blockchain = 'arbitrum'
GROUP BY 1, 2