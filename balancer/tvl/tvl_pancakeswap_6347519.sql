-- part of a query repo
-- query name: TVL PancakeSwap
-- query link: https://dune.com/queries/6347519


with tvl as (select 
    block_date, 
    token0_balance_usd, 
    token1_balance_usd, 
    token0_balance_usd + token1_balance_usd as TVL 
from pancakeswap.tvl_daily 
WHERE 
    id = 0x80ceb98632409080924dce50c26acc25458dde17 AND 
    block_date >= TIMESTAMP '2025-01-01 00:00:00') SELECT avg(TVL) from tvl