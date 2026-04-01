-- part of a query repo
-- query name: veBAL rounds
-- query link: https://dune.com/queries/4001811


WITH weeks_seq AS (
        SELECT sequence(CAST('2022-04-07' AS timestamp), cast(now() as timestamp), interval '7' day) as week
    ),

    calendar AS (
        SELECT weeks.week AS start_date FROM weeks_seq CROSS JOIN unnest(week) as weeks(week)
    )

        SELECT
            start_date,
            to_unixtime(start_date) AS start_timestamp,
            start_date + interval '7' day AS end_date,
            to_unixtime(start_date + interval '7' day) AS end_timestamp,
            row_number() OVER (ORDER BY start_date) AS round_id
        FROM calendar