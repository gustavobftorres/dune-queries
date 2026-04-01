-- part of a query repo
-- query name: Aave token dex swap information
-- query link: https://dune.com/queries/6341548


with categorized_trades as (
    select
        block_date
        , blockchain
        , project
        , version
        , token_pair
        , project_contract_address
        , amount_usd
        , case
when amount_usd < 100
    then 100
when amount_usd < 1000
    then 1000
when amount_usd < 2500
    then 2500
when amount_usd < 3500
    then 3500
when amount_usd < 5000
    then 5000
when amount_usd < 10000
    then 10000
when amount_usd < 20000
    then 20000
when amount_usd < 30000
    then 30000
when amount_usd < 40000
    then 40000
when amount_usd < 50000
    then 50000
when amount_usd < 60000
    then 60000
when amount_usd < 70000
    then 70000
when amount_usd < 80000
    then 80000
when amount_usd < 90000
    then 90000
when amount_usd < 200000
    then 200000
else
    200001
end as category
        from dex.trades
        where block_time >= current_date - INTERVAL '30' DAY
   and amount_usd > 0
   and blockchain in ('ethereum')
   and (
                LOWER(token_bought_symbol) like '%aave%'
   or LOWER(token_sold_symbol) like '%aave%'
)
)
select
    block_date
    , blockchain
    , project
    , version
    , token_pair
    , project_contract_address
    , category
    , count(*) as n_swaps
    , sum(amount_usd) as volume
from categorized_trades
group by
    1
    , 2
    , 3
    , 4
    , 5
    , 6
    , 7
order by
    block_date desc
    , volume desc