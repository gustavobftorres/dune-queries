-- part of a query repo
-- query name: BAL Supply Projection (DuneSQL)
-- query link: https://dune.com/queries/2846023


WITH 
    part_a AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY day) -1 AS rn,
            date_trunc('day', day) AS time,
            CAST('48982142857142857143000000' AS DOUBLE)/POWER(10,18) AS innit_supply,
            CAST('239748677248677248' AS DOUBLE)/POWER(10,18) AS rate,
            CAST('1189207115002721024' AS DOUBLE)/POWER(10,18) AS rate_redu_coef,
            86400 AS sec_in_day,
            date_diff('year', CAST('2022-03-28 11:00:51' AS timestamp), day) AS epoch
        FROM (
            WITH 
                days_seq AS (
                    SELECT
                        SEQUENCE(
                            CAST('2022-03-28 11:00:51' AS timestamp),
                            (CAST(NOW() AS timestamp) + INTERVAL '10' YEAR),
                            INTERVAL '1' DAY) AS day
                )
                SELECT 
                    days.day AS day
                FROM days_seq
                CROSS JOIN unnest(day) AS days(day)
        )
    ),
    
    part_b AS (
        SELECT *,
            CASE
                WHEN epoch = 0 THEN rate * sec_in_day
                WHEN epoch > 0 THEN (rate/POWER(rate_redu_coef, epoch)) * sec_in_day
            END AS day_rate
        from part_a
    ),
    
    part_c AS (
        SELECT *, day_rate * 7 AS week_rate, 'veBAL' AS tokenomics,
            CASE 
                WHEN rn = 0 THEN innit_supply
                WHEN rn > 0 THEN (SUM(day_rate) OVER (ORDER BY time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) + innit_supply)
            END AS max_circ_supply
        FROM part_b
    )
    
    SELECT * FROM part_c ORDER BY time ASC

