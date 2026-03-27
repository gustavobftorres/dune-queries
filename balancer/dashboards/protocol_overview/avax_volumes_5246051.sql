-- part of a query repo
-- query name: avax volumes
-- query link: https://dune.com/queries/5246051


select token_pair, sum(amount_usd) as volume_usd
from dex.trades
where blockchain = 'avalanche_c'
and block_date >= timestamp '2025-01-01'
group by 1
order by 2 desc