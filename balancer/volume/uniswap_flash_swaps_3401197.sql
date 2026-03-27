-- part of a query repo
-- query name: Uniswap flash swaps
-- query link: https://dune.com/queries/3401197


select 'uniswapV2Call' as function, get_href(get_chain_explorer_address('ethereum', "to"), cast("to" as varchar)) as flashswapper, count(1) as n_calls from ethereum.traces 
where starts_with(input, 0x10d1e85c)
and block_date > now() - interval '30' day
group by 1, 2
order by 3 desc

-- union all

-- select 'uniswapV3FlashCallback' as version, "to" as flashswapper, count(1) as n_calls from ethereum.traces 
-- where starts_with(input, 0xe9cbafb0)
-- and block_date > now() - interval '30' day
-- group by 1, 2

-- union all

-- select 'uniswapV3SwapCallback' as version, "to" as flashswapper, count(1) as n_calls from ethereum.traces 
-- where starts_with(input, 0xfa461e33)
-- and block_date > now() - interval '30' day
-- group by 1, 2