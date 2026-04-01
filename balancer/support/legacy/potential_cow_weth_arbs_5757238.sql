-- part of a query repo
-- query name: potential cow/weth arbs
-- query link: https://dune.com/queries/5757238


select *
from dex.trades
where blockchain = 'base'
and project_contract_address = 0xff028c1ec4559d3aa2b0859aa582925b5cc28069
and tx_hash not in (
    select tx_hash
    from dex_aggregator.trades
    where blockchain = 'base'
    and block_date >= timestamp '2025-07-10'
)
and tx_to not in (
    0x6A000F20005980200259B80c5102003040001068
)
order by amount_usd desc
