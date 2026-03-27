-- part of a query repo
-- query name: micro swaps on v2
-- query link: https://dune.com/queries/3927337


with v2_swaps as (
    select
        *,
        log10(token_sold_amount_raw / token_sold_amount) as token_sold_decimals,
        log10(token_bought_amount_raw / token_bought_amount) as token_bought_decimals
    from dex.trades
    where project = 'balancer'
    and version = '2'
),

swaps_scaled18 as (
    select
        *,
        token_sold_amount_raw * power(10, 18 - token_sold_decimals) as token_sold_amount_scaled18,
        token_bought_amount_raw * power(10, 18 - token_bought_decimals) as token_bought_amount_scaled18
    from v2_swaps
),

micro_swaps as (
    select *
    from swaps_scaled18
    where (token_bought_amount_scaled18 < 1e6)
    or (token_sold_amount_scaled18 < 1e6)
),

all_swaps as (
    select blockchain, count(*) as total_swaps, sum(amount_usd) as total_swaps_volume
    from swaps_scaled18
    group by 1
)

select 
    m.blockchain,
    total_swaps,
    total_swaps_volume,
    count(*) as micro_swaps,
    count(case when token_bought_amount_raw = 0 then 1 end) as micro_swaps_zero_bought,
    count(case when token_bought_amount_raw > 0 then 1 end) as micro_swaps_positive_bought,
    sum(amount_usd) as micro_swaps_volume
from micro_swaps m
join all_swaps a
on m.blockchain = a.blockchain
group by 1, 2, 3
order by 4 desc
