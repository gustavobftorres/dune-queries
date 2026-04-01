-- part of a query repo
-- query name: 0x1db1 sandwich attacker
-- query link: https://dune.com/queries/961088


with bread as (
    select * from ethereum.transactions 
    where "to" = '\x1db1c0e2829e69275bc2b54e65ac5b06683e5cde'
    and block_time > '06-07-2022'
),
bottom as (
    select block_number, min(index) as index
    from bread
    group by 1
),
filling as (
    select a.* from ethereum.transactions a
    inner join bottom b
    on a.block_number = b.block_number
    and a.index = b.index + 1
    and block_time > '06-07-2022'
),
sandwiches as (
    select *, 'bot' as who from bread
    union all
    select *, 'user' as who from filling
)
select 
    a.block_number,
    a.index as tx_index,
    a.who,
    c.symbol as "tokenIn",
    d.symbol as "tokenOut",
    b."amountIn"/1e18 as "amountIn",
    b."amountOut"/1e18 as "amountOut"
from sandwiches a
inner join balancer_v2."Vault_evt_Swap" b
on a."hash" = b.evt_tx_hash
and b."poolId" = '\x7b50775383d3d6f0215a8f290f2c9e2eebbeceb20000000000000000000000fe'
inner join erc20.tokens c
on b."tokenIn" = c.contract_address
inner join erc20.tokens d
on b."tokenOut" = d.contract_address
order by block_number, index
