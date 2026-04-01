-- part of a query repo
-- query name: Root Gauges with votes (and kill status)
-- query link: https://dune.com/queries/2164855


-- TODO: add new root gauges when decoded

WITH last_thursday AS (
    SELECT timestamp '2023-08-24' as last_thursday
),
previous_thursday AS (
    SELECT (last_thursday - interval '7' DAY) as previous_thursday from last_thursday
),
root_gauges as (
    select gauge, 'polygon' as network, recipient from balancer_ethereum.PolygonRootGaugeFactory_evt_PolygonRootGaugeCreated
    union all
    select gauge, 'arbitrum' as network, recipient from balancer_ethereum."ArbitrumRootGaugeFactory_evt_ArbitrumRootGaugeCreated"
    union all
    select gauge, 'optimism' as network, recipient from balancer_ethereum."OptimismRootGaugeFactory_evt_OptimismRootGaugeCreated"
    union all
    select "output_0" as gauge, 'polygon' as network, recipient from balancer_ethereum."CappedPolygonRootGaugeFactory_call_create"
    union all
    select "output_0" as  gauge, 'arbitrum' as network, recipient from balancer_ethereum."CappedArbitrumRootGaugeFactory_call_create"
    union all
    select "output_0" as  gauge, 'optimism' as network, recipient from balancer_ethereum."CappedOptimismRootGaugeFactory_call_create"
    union all
    select "output_0" as  gauge, 'gnosis' as network, recipient from balancer_ethereum."GnosisRootGaugeFactory_call_create"
),
killed_gauges as (
    with calls as (
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."ArbitrumRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."ArbitrumRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."OptimismRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."OptimismRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."GnosisRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."GnosisRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."PolygonRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."PolygonRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."CappedArbitrumRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."CappedArbitrumRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."CappedOptimismRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."CappedOptimismRootGauge_call_unkillGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'kill' as call_type 
        from balancer_ethereum."CappedPolygonRootGauge_call_killGauge" where call_success
        union all
        select call_block_time, contract_address as gauge, 'unkill' as call_type 
        from balancer_ethereum."CappedPolygonRootGauge_call_unkillGauge" where call_success
    ),
    most_recent_call as (
        select gauge, max(call_block_time) as call_block_time from calls
        where gauge <> 0x3b8ca519122cdd8efb272b0d3085453404b25bd0
        group by 1
    )
    select c1.gauge from calls c1
    inner join most_recent_call c2
    on c1.gauge = c2.gauge
    and c1.call_block_time = c2.call_block_time
    and c1.call_type = 'kill'
),
prev_week_total_votes as (
    SELECT sum(v.vote) as total_votes
    FROM balancer_ethereum.vebal_votes v
    inner join previous_thursday on 1=1
    WHERE end_date = previous_thursday.previous_thursday
    and v.vote>0
),
prev_week as (
    SELECT rg.*, prev_week_total_votes.total_votes, sum(v.vote) as votes, sum(v.vote)/prev_week_total_votes.total_votes as rel_weight
    FROM root_gauges rg
    left join balancer_ethereum.vebal_votes v
    on v.gauge = rg.gauge
    inner join previous_thursday on 1=1
    inner join prev_week_total_votes on 1=1
    WHERE end_date = previous_thursday.previous_thursday
    and v.vote>0
    group by 1,2,3,4
),
this_week_total_votes as (
    SELECT sum(v.vote) as total_votes
    FROM balancer_ethereum.vebal_votes v
    inner join last_thursday on 1=1
    WHERE end_date = last_thursday.last_thursday
    and v.vote>0
),
this_week as (
    SELECT rg.*, this_week_total_votes.total_votes, sum(v.vote) as votes, sum(v.vote)/this_week_total_votes.total_votes as rel_weight
    FROM root_gauges rg
    left join balancer_ethereum.vebal_votes v
    on v.gauge = rg.gauge
    inner join last_thursday on 1=1
    inner join this_week_total_votes on 1=1
    WHERE end_date = last_thursday.last_thursday
    and v.vote>0
    group by 1,2,3,4
),
votes as (
    select 
        coalesce(prev_week.gauge, this_week.gauge) as gauge,
        coalesce(prev_week.network, this_week.network) as network,
        coalesce(prev_week.recipient, this_week.recipient) as recipient,
        coalesce(prev_week.votes, 0) as votes_prev_week, 
        coalesce(prev_week.rel_weight, 0) as rel_weight_prev_week, 
        coalesce(this_week.votes, 0) as votes_this_week,
        coalesce(this_week.rel_weight, 0) as rel_weight_this_week
    from prev_week
    full outer join this_week 
    on prev_week.gauge = this_week.gauge
    order by network, gauge
),
votes_and_kill_status as (
    select 
        v.*, 
        case when killed_gauges.gauge is not null then 'killed' else 'ok' end as status
    from votes v
    left join killed_gauges
    on v.gauge = killed_gauges.gauge
)
select * from votes_and_kill_status
where status = 'ok'
order by network, votes_this_week desc