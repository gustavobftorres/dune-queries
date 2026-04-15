-- part of a query repo
-- query name: pmusd cow orders
-- query link: https://dune.com/queries/6916137


select 
    agg.token_pair,
    sum(agg.amount_usd) as volume_usd
from dex.trades dex
inner join dex_aggregator.trades agg
    on dex.tx_hash = agg.tx_hash
    and agg.blockchain = 'ethereum'
where dex.blockchain = 'ethereum'
    and dex.project_contract_address = 0xecb0f0d68c19bdaadaebe24f6752a4db34e2c2cb -- curve pool 
    and dex.tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41 -- cow settlement
group by 1
order by 2 desc
