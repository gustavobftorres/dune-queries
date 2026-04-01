-- part of a query repo
-- query name: TUSD Stable Pool - Weekly Volume
-- query link: https://dune.com/queries/274924


select date_trunc('week', block_time) AS week, sum(usd_amount) AS "Volume"
from dex.trades
where exchange_contract_address = '\x0d34e5dd4d8f043557145598e4e2dc286b35fd4f000000000000000000000068'
and block_time >= '2021-11-15'
group by 1