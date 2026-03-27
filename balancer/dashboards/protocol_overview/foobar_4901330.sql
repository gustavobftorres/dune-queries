-- part of a query repo
-- query name: foobar
-- query link: https://dune.com/queries/4901330


select block_month, case when tx_to = 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 then 'cow' else 'others' end as source, sum(amount_usd) as volume
from dex.trades
where project = 'uniswap'
and blockchain = 'ethereum'
and project_contract_address = 0x5ab53ee1d50eef2c1dd3d5402789cd27bb52c1bb
group by 1, 2
