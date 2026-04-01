-- part of a query repo
-- query name: ve8020 Pool LPs
-- query link: https://dune.com/queries/3102567


WITH joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e."to" AS lp, e.contract_address AS pool, SUM(CAST(e.value as double))/1e18 AS amount
        FROM balancer_v2_{{Blockchain}}.transfers_bpt e
        WHERE e."to" != 0x0000000000000000000000000000000000000000
        GROUP BY 1, 2, 3
    ),
    
    exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e."from" AS lp, e.contract_address AS pool, -SUM(CAST(e.value as double))/1e18 AS amount
        FROM balancer_v2_{{Blockchain}}.transfers_bpt e
        WHERE e."from" != 0x0000000000000000000000000000000000000000
        GROUP BY 1, 2, 3
    ),
    
    daily_delta_bpt_by_pool AS (
        SELECT day, lp, pool, SUM(COALESCE(amount, 0)) as amount FROM 
        (SELECT *
        FROM joins j 
        UNION ALL
        SELECT * 
        FROM exits e) foo
        GROUP BY 1, 2, 3
    ),
    
    cumulative_bpt_by_pool AS (
        SELECT day, lp, pool, amount, 
        LEAD(day, 1, CURRENT_DATE) OVER (PARTITION BY lp, pool ORDER BY day) AS next_day,
        SUM(amount) OVER (PARTITION BY lp, pool ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS amount_bpt
        FROM daily_delta_bpt_by_pool
    ),
    
calendar AS (

    with days_seq as (
        SELECT
        sequence(
            (SELECT cast(min(date_trunc('day', evt_block_time)) as timestamp) day FROM erc20_ethereum.evt_Transfer tr)
            , date_trunc('day', cast(now() as timestamp))
            , interval '1' day) as day
    )
    
    SELECT 
        days.day
    FROM days_seq
    CROSS JOIN unnest(day) as days(day)),
    
   running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
        WHERE pool = {{Pool Address}}
    )
    
SELECT COUNT(DISTINCT lp) AS lps
FROM running_cumulative_bpt_by_pool
WHERE day = CURRENT_DATE - interval '1' day
AND amount_bpt > 0