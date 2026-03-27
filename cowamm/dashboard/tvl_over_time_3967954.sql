-- part of a query repo
-- query name: cow amms total tvl v2
-- query link: https://dune.com/queries/3967954


-- computes total tvl over time and the 7 day growth
-- can be used to get the last tvl/growth in a counter for a dashboard

with tvl as (
    select
        day,
        sum(value0 + value1) as tvl
    from dune.cowprotocol.result_amm_lp_infos
    where project = 'cow_amm' and value0 < 1000000000 and value1 < 1000000000
    group by day
)

select
    curr.day,
    prev.tvl as prev,
    curr.tvl as curr,
    100 * (curr.tvl - prev.tvl) / prev.tvl as growth
from tvl as curr
inner join tvl as prev
    on curr.day = prev.day + interval '7' day
where not (curr.tvl is null)
order by day desc
