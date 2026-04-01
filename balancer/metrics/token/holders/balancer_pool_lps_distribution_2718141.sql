-- part of a query repo
-- query name: Balancer Pool LPs Distribution
-- query link: https://dune.com/queries/2718141


WITH joins AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e."to" AS lp, e.contract_address AS pool, SUM(CAST(e.value as double))/1e18 AS amount
        FROM balancer_v2_{{4. Blockchain}}.transfers_bpt e
        WHERE e."to" != 0x0000000000000000000000000000000000000000
        GROUP BY 1, 2, 3
    ),
    
    exits AS (
        SELECT date_trunc('day', e.evt_block_time) AS day, e."from" AS lp, e.contract_address AS pool, -SUM(CAST(e.value as double))/1e18 AS amount
        FROM balancer_v2_{{4. Blockchain}}.transfers_bpt e
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
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2020-03-01'), date(now()), interval '1' day)) as t(date_sequence)
    ),
    
   running_cumulative_bpt_by_pool as (
        SELECT c.day, lp, pool, amount_bpt
        FROM cumulative_bpt_by_pool b
        JOIN calendar c on b.day <= c.day AND c.day < b.next_day
        WHERE CAST (pool as varchar) = SUBSTRING('{{1. Pool ID}}',1,42)),
    
    
  total_amount_bpt AS (
        SELECT day, pool, SUM(amount_bpt) AS total_bpt
        FROM running_cumulative_bpt_by_pool
        WHERE day = CURRENT_DATE - interval '1' day
        GROUP BY 1, 2
    )

    SELECT
        CASE WHEN r.amount_bpt / tb.total_bpt > 0.01 THEN CAST(r.lp as varchar)
        ELSE 'others'
        END AS labeled_lp, r.amount_bpt
    FROM running_cumulative_bpt_by_pool r
    JOIN total_amount_bpt tb ON r.day = tb.day AND r.pool = tb.pool
    WHERE r.day = CURRENT_DATE - interval '1' day AND r.amount_bpt > 0