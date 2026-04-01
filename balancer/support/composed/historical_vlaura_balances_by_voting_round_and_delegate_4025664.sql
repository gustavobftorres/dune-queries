-- part of a query repo
-- query name: historical vlAURA balances by voting round and delegate
-- query link: https://dune.com/queries/4025664


WITH ranked_delegates AS (
    SELECT
        vlaura_round,
        delegate,
        SUM(total_locked) AS total_locked,
        SUM(voting_power) AS voting_power,
        ROW_NUMBER() OVER (PARTITION BY vlaura_round ORDER BY SUM(total_locked) DESC) AS rn
    FROM query_4023187
    GROUP BY 1, 2
),
top_10_and_others AS (
    SELECT
        vlaura_round,
        CASE 
            WHEN rn <= 10 THEN CAST(delegate AS VARCHAR)
            ELSE 'Others'
        END AS delegate_group,
        SUM(total_locked) AS total_locked,
        SUM(voting_power) AS voting_power
    FROM ranked_delegates
    GROUP BY 1, 2
)

SELECT
    vlaura_round,
    delegate_group AS delegate,
    total_locked,
    voting_power
FROM top_10_and_others
ORDER BY vlaura_round, total_locked DESC