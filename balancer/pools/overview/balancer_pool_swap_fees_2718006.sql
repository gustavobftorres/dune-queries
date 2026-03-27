-- part of a query repo
-- query name: Balancer Pool Swap Fees
-- query link: https://dune.com/queries/2718006


WITH swap_fees AS (
    SELECT
        date_trunc('day', block_time) AS day,
        swap_fee_percentage/1e18 AS swap_fee,
        contract_address AS pool
    FROM balancer.pools_fees
    WHERE CAST(contract_address as VARCHAR) = SUBSTRING('{{1. Pool ID}}', 1 ,42) AND blockchain = '{{4. Blockchain}}'
),


swap_fee_with_gaps AS(
    SELECT 
        day,
        swap_fee,
        LEAD(day, 1, now()) OVER (ORDER BY day) AS next_day,
        pool
    FROM swap_fees
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
    CROSS JOIN unnest(day) as days(day))

SELECT
    c.day,
    f.swap_fee,
    f.pool
FROM calendar c
LEFT JOIN swap_fee_with_gaps f ON f.day <= c.day AND c.day < f.next_day
ORDER BY c.day DESC