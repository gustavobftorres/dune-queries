-- part of a query repo
-- query name: Aggregators Volume over DEXs
-- query link: https://dune.com/queries/3628916


with data as (
    select tx_to, token_pair, case when (project='balancer') then 'Balancer' else  'Others' end as project, date_trunc('month', block_date) as week, sum(amount_usd) as volume
    from dex.trades 
    where 1=1
    and tx_to IN (CASE
        WHEN '{{aggregator}}' = 'Fusion' THEN 0xad3b67bca8935cb510c8d18bd45f0b94f54a968f -- Fusion
        WHEN '{{aggregator}}' = 'CoW' THEN 0x9008D19f58AAbD9eD0D60971565AA8510560ab41 -- CoW
        END)
    and token_pair = case when '{{Token Pair}}' = 'all' then token_pair else '{{Token Pair}}' end
    and block_date > now() - interval '12' month
    and blockchain = '{{blockchain}}'
    group by 1,2,3,4
)
-- select token_pair, day, sum(volume)
-- select ARRAY[project] || token_pair, day, sum(volume)
select project, week, sum(volume)
from data
WHERE 1=1
-- and project = 'Balancer'
-- and volume > 100000
group by 1,2