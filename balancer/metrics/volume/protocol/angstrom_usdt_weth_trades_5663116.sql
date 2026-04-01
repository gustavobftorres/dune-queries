-- part of a query repo
-- query name: angstrom usdt/weth trades
-- query link: https://dune.com/queries/5663116


select block_time, block_number, token_bought_symbol, token_sold_symbol, amount_usd, tx_from, tx_hash
from dex.trades
where project = 'uniswap'
and blockchain = 'ethereum'
and version = '4'
and maker = 0x90078845bceb849b171873cfbc92db8540e9c803ff57d9d21b1215ec158e79b3
order by block_time desc
