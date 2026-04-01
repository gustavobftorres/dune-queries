-- part of a query repo
-- query name: B-80BAL-20WETH Locked by Top LPs
-- query link: https://dune.com/queries/544023


with vebal_evts as (
        select date_trunc('day', evt_block_time) as day, provider, value / 1e18 vebal
        from balancer."veBAL_evt_Deposit"
        union all
        select date_trunc('day', evt_block_time) as day, provider, -(value / 1e18) vebal
        from balancer."veBAL_evt_Withdraw"
    ),
    
    delta_vebal as (
        select day, provider, sum(vebal) as vebal
        from vebal_evts
        group by 1,2
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT day, provider, SUM(vebal) OVER (PARTITION BY provider ORDER BY day) AS vebal, 
        LEAD(day, 1, now()) OVER (PARTITION BY provider ORDER BY day) AS next_day
        FROM delta_vebal
    ),
    
    calendar AS (
        SELECT generate_series('2022-03-28'::timestamp, CURRENT_DATE, '1 day'::interval) AS day
    ),
    
   running_cumulative_bpt_by_pool as (
        SELECT c.day, provider, vebal
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
    ),
    
    total_vebal as (
        select day, sum(vebal) as total_vebal
        from running_cumulative_bpt_by_pool
        group by 1
    ),
    
    top_lps as (
        select provider
        from running_cumulative_bpt_by_pool
        where day = CURRENT_DATE
        order by vebal desc
        limit 10
    )

select 
    r.day,
    coalesce(t.provider::text, 'Others') as provider,
    sum(vebal),
    sum(vebal) / total_vebal as pct
from running_cumulative_bpt_by_pool r
inner join total_vebal v on v.day = r.day
left join top_lps t on t.provider = r.provider
group by 1, 2, total_vebal