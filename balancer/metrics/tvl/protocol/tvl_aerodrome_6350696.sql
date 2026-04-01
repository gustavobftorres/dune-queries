-- part of a query repo
-- query name: TVL Aerodrome
-- query link: https://dune.com/queries/6350696


with tvl as (select 
    block_date, 
    token0_balance_usd, 
    token1_balance_usd, 
    token0_balance_usd + token1_balance_usd as TVL 
from aerodrome.tvl_daily 
WHERE 
    id = 0x4a79b0168296c0ef7b8f314973b82ad406a29f1b AND 
    block_date >= TIMESTAMP '2025-01-01 00:00:00') SELECT avg(TVL) from tvl