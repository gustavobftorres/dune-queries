-- part of a query repo
-- query name: Fluid DEX daily data
-- query link: https://dune.com/queries/6587926


with

fluid_blockchains as (
    select *
    from "query_5044493"
)

, fluid_dexes as (
    select *
    from dune."0xfluid".result_fluid_dexes
)

, tokens_native as (
    select *
    from "query_5825281"
)

, manual_pricing as (
    select *
    from "query_5169345"
)

, dex_tokens_subset as (
    select
          token0 as contract_address
        , blockchain
        , symbol0 as symbol
        , decimals0 as decimals
    from fluid_dexes
    union
    select
          token1 as contract_address
        , blockchain
        , symbol1 as symbol
        , decimals1 as decimals
    from fluid_dexes
)

, dex_tokens as (
    select
          t.contract_address as token
        , t.blockchain
        , t.symbol
        , t.decimals
        , t.contract_address as price_token
        , t.blockchain as price_blockchain
        , t.decimals as price_decimals
    from dex_tokens_subset as t
    left join manual_pricing as mp
        on t.contract_address = mp.token
            and t.blockchain = mp.blockchain
    where t.contract_address <> 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
        and t.blockchain in (select blockchain from fluid_blockchains)
        and mp.token is null

    union all

    select
          0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee as token
        , chain as blockchain
        , symbol
        , decimals
        , price_address as price_token
        , chain as price_blockchain
        , decimals as price_decimals
    from tokens_native
    where chain in (select blockchain from fluid_blockchains)

    union all

    select
          t.contract_address as token
        , t.blockchain as blockchain
        , t.symbol
        , t.decimals
        , mp.price_token as price_token
        , mp.price_blockchain as price_blockchain
        , mp.price_decimals as price_decimals
    from dex_tokens_subset as t
    inner join manual_pricing as mp
        on t.contract_address = mp.token
            and t.blockchain = mp.blockchain
)

, asset_prices_ext as (
    select
          date_trunc('hour', p.minute) as hour
        , t.token as token
        , t.blockchain
        , approx_percentile(p.price, 0.5) as price
    from prices.usd as p
    inner join dex_tokens as t
    on p.contract_address = t.price_token
        and p.blockchain = t.price_blockchain
    where p.minute >= timestamp '2026-01-01'
    group by 1, 2, 3
)

, fluid_dex_fee_update as (
    select *
    from dune."0xfluid".result_fluid_dex_fee_update
)

, fluid_swaps as (
    select
          tx_hash
        , block_time
        , s.hour
        , s.day
        , s.block_number
        , s.index
        , s.blockchain
        , dex
        , dex_id
        , dex_name
        , token0
        , token1
        , receiver
        , tx_from
        , cast(amount_in_raw as double) / if(swap0to1,  pow(10, t0.decimals), pow(10, t1.decimals)) as amount_in
        , if(swap0to1, t0.token, t1.token) as token_in
        , if(swap0to1, t0.symbol, t1.symbol) as symbol_in
        , if(swap0to1, cast(amount_in_raw as double) / pow(10, t0.decimals) * p0.price, cast(amount_in_raw as double) / pow(10, t1.decimals) * p1.price) as amount_in_usd
        , if(swap0to1, p0.price, p1.price) as token_in_price
        , cast(amount_out_raw as double) / if(swap0to1,  pow(10, t1.decimals), pow(10, t0.decimals)) as amount_out
        , if(swap0to1, t1.token, t0.token) as token_out
        , if(swap0to1, t1.symbol, t0.symbol) as symbol_out
        , if(swap0to1, cast(amount_out_raw as double) / pow(10, t1.decimals) * p1.price, cast(amount_out_raw as double) / pow(10, t0.decimals) * p0.price) as amount_out_usd
        , if(swap0to1, p1.price, p0.price) as token_out_price
    from (
       select
              l.evt_tx_hash as tx_hash
            , l.evt_block_time as block_time
            , date_trunc('hour', l.evt_block_time) as hour
            , evt_block_date as day
            , l.evt_block_number as block_number
            , l.evt_index as index
            , l.chain as blockchain
            , d.dex
            , d.dex_id
            , d.dex_name
            , d.token0
            , d.token1
            , l.swap0to1
            , l.amountIn as amount_in_raw
            , l.amountOut as amount_out_raw
            , l.to as receiver
            , l.evt_tx_from as tx_from
        from fluid_multichain.fluiddext1_evt_swap as l
        inner join fluid_dexes as d
            on l.contract_address = d.dex
                and l.chain = d.blockchain
    ) as s
    inner join dex_tokens as t0
        on s.token0 = t0.token
             and s.blockchain = t0.blockchain
    inner join dex_tokens as t1
        on s.token1 = t1.token
             and s.blockchain = t1.blockchain
    left join asset_prices_ext as p0
        on t0.token = p0.token
             and t0.blockchain = p0.blockchain
             and s.hour = p0.hour
    left join asset_prices_ext as p1
        on t1.token = p1.token
             and t1.blockchain = p1.blockchain
             and s.hour = p1.hour
)

, fluid_swaps_with_fees as (
    select
          *
        , amount_in * fee as fee_in
        , amount_in_usd * fee as fee_in_usd
        , amount_in * fee * revenue_cut as revenue_in
        , amount_in_usd * fee * revenue_cut as revenue_in_usd
        , amount_out * fee as fee_out
        , amount_out_usd * fee as fee_out_usd
        , amount_out * fee * revenue_cut as revenue_out
        , amount_out_usd * fee * revenue_cut as revenue_out_usd
    from (
        select
              s.*
            , f.fee
            , f.revenue_cut
            , row_number() over (partition by s.tx_hash, s.index order by f.block_number desc, f.index desc) as rn
        from fluid_swaps as s
        left join fluid_dex_fee_update as f
        on f.dex = s.dex
            and f.blockchain = s.blockchain
            and (f.block_number < s.block_number or (f.block_number = s.block_number and f.index < s.index)))
    where rn = 1
)

-- DAILY AGGREGATION
select
      day
    , blockchain
    , dex
    , dex_id
    , dex_name
    , token0
    , token1
    , count(*) as num_swaps
    , count(distinct tx_from) as unique_traders
    , count(distinct tx_hash) as unique_transactions
    -- Aggregated amounts IN
    , sum(amount_in) as total_amount_in
    , sum(amount_in_usd) as total_amount_in_usd
    -- Aggregated amounts OUT
    , sum(amount_out) as total_amount_out
    , sum(amount_out_usd) as total_amount_out_usd
    -- Aggregated fees IN
    , sum(fee_in) as total_fee_in
    , sum(fee_in_usd) as total_fee_in_usd
    , sum(revenue_in) as total_revenue_in
    , sum(revenue_in_usd) as total_revenue_in_usd
    -- Aggregated fees OUT
    , sum(fee_out) as total_fee_out
    , sum(fee_out_usd) as total_fee_out_usd
    , sum(revenue_out) as total_revenue_out
    , sum(revenue_out_usd) as total_revenue_out_usd
    -- Average fee rate (weighted by USD volume)
    , sum(fee * amount_in_usd) / nullif(sum(amount_in_usd), 0) as avg_fee_rate_weighted
    , sum(revenue_cut * amount_in_usd) / nullif(sum(amount_in_usd), 0) as avg_revenue_cut_weighted
from fluid_swaps_with_fees
group by
      day
    , blockchain
    , dex
    , dex_id
    , dex_name
    , token0
    , token1
order by day desc, blockchain, dex
