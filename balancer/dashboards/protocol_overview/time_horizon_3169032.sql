-- part of a query repo
-- query name: time horizon
-- query link: https://dune.com/queries/3169032


WITH
    calendar as (
        WITH   
          -- 1 Min Calendar 
            "1 MIN" AS (
                SELECT
                    date_add('minute', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '1' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '1' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1440, 1)) AS t(step) )
                WHERE date_add('minute', step, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 5 Min Calendar 
            "5 MIN" AS (
                SELECT
                    date_add('minute', step * 5, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '5' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '5' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 288, 1)) AS t(step) )
                WHERE date_add('minute', step * 5, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 15 Min Calendar 
            "15 MIN" AS (
                SELECT
                    date_add('minute', step * 15, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '15' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '15' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 96, 1)) AS t(step) )
                WHERE date_add('minute', step * 15, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 30 Min Calendar 
            "30 MIN" AS (
                SELECT
                    date_add('minute', step * 30, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '30' minute),
                            (date_trunc('minute', cast(now() AS TIMESTAMP)) + INTERVAL '30' minute),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 48, 1)) AS t(step) )
                WHERE date_add('minute', step * 30, day) <= date_trunc('minute', cast(now() AS TIMESTAMP))
            ),
            -- 1 Hour calendar
            "1 HOUR" AS (
                SELECT
                    date_add('hour', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '1' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '1' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 24, 1)) AS t(step) )
                WHERE date_add('hour', step, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 2 Hour calendar
            "2 HOUR" AS (
                SELECT
                    date_add('hour', step * 2, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '2' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '2' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM  UNNEST(SEQUENCE(1, 12, 1)) AS t(step) )
                WHERE date_add('hour', step * 2, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 4 Hour calendar
            "4 HOUR" AS (
                SELECT
                    date_add('hour', step * 4, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '4' hour),
                            (date_trunc('hour', cast(now() AS TIMESTAMP)) + INTERVAL '4' hour),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 6, 1)) AS t(step) )
                WHERE date_add('hour', step * 4, day) <= date_trunc('hour', cast(now() AS TIMESTAMP))
            ),
            -- 1 Day calendar
            "1 DAY" AS (
                SELECT
                    date_add('day', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('day', cast((now() - INTERVAL '{{Time Frame in Time Units}}' day) AS TIMESTAMP)) - INTERVAL '1' day),
                            (date_trunc('day', cast(now() AS TIMESTAMP)) + INTERVAL '1' day),
                            INTERVAL '1' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1, 1)) AS t(step) )
                WHERE date_add('day', step, day) <= date_trunc('day', cast(now() AS TIMESTAMP))
            ),
            -- 1 WEEK calendar
            "1 WEEK" AS (
                SELECT
                    date_add('week', step, day) AS minute
                FROM
                    UNNEST(
                        SEQUENCE(
                            (date_trunc('week', cast((now() - (INTERVAL '{{Time Frame in Time Units}}' day) * 7) AS TIMESTAMP)) - INTERVAL '7' day),
                            (date_trunc('week', cast(now() AS TIMESTAMP)) + INTERVAL '7' day),
                            INTERVAL '7' day
                        )
                    ) AS t(day)
                CROSS JOIN ( SELECT * FROM UNNEST(SEQUENCE(1, 1, 1)) AS t(step) )
                WHERE date_add('week', step, day) <= date_trunc('week', cast(now() AS TIMESTAMP))
            )
            
        
        SELECT * FROM "{{Time Unit}}"
    )

SELECT 
    block_time, week(block_time) FROM balancer_v2_base.trades limit 1