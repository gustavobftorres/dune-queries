-- part of a query repo
-- query name: 1inch/CoW Volume over DEXs
-- query link: https://dune.com/queries/3054511


select date_trunc('day', block_date) as day, case when (project='balancer') then 'Balancer' else  'Others' end as project, sum(amount_usd) as volume
from dex.trades 
where 1=1
and tx_to IN (0xad3b67bca8935cb510c8d18bd45f0b94f54a968f, 0x9008D19f58AAbD9eD0D60971565AA8510560ab41)
and block_date > now() - interval '6' month
and ('{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}')
group by 1, 2