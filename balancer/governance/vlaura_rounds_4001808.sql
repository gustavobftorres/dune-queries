-- part of a query repo
-- query name: vlAURA rounds
-- query link: https://dune.com/queries/4001808


WITH weeks_seq AS (
        SELECT sequence(CAST('2022-06-16' AS timestamp), cast(now() as timestamp), interval '7' day) as week
    ),

    calendar AS (
        SELECT weeks.week AS start_date FROM weeks_seq CROSS JOIN unnest(week) as weeks(week)
    ),
    
    rounds_info AS (
        SELECT
            start_date,
            to_unixtime(start_date) AS start_timestamp,
            start_date + interval '7' day AS end_date,
            to_unixtime(start_date + interval '7' day) AS end_timestamp,
            row_number() OVER (ORDER BY start_date) AS round_id
        FROM calendar
    )
    
    SELECT 
        a.start_date,
        a.start_timestamp,
        b.end_date,
        b.end_timestamp,
        a.round_id AS vlaura_round,
        b.round_id AS vebal_round 
    FROM rounds_info a
    LEFT JOIN query_4001811 b
    ON a.start_date = b.start_date
    AND a.end_date = b.end_date
