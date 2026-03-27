-- part of a query repo
-- query name: Historical veBAL balances (vlAURA providers breakdown)
-- query link: https://dune.com/queries/4025600


WITH vebal_vlaura_join AS (
    SELECT 
        vebal_round,
        provider,
        vebal_balance
    FROM query_601405 qa
    LEFT JOIN query_4001808 rounds
    ON qa.day = rounds.end_date
    WHERE qa.provider != 'Aura'

    UNION ALL

    SELECT
        vebal_round,
        provider,
        SUM(vebal_balance) AS vebal_balance
    FROM query_4023187
    GROUP BY 1, 2
),

ranked_providers AS (
    SELECT 
        vebal_round,
        provider,
        SUM(vebal_balance) AS vebal_balance,
        ROW_NUMBER() OVER (PARTITION BY vebal_round ORDER BY SUM(vebal_balance) DESC) AS rn
    FROM vebal_vlaura_join
    GROUP BY vebal_round, provider
),

top_10_and_others AS (
    SELECT 
        vebal_round,
        CASE 
            WHEN rn <= 10 THEN provider
            ELSE 'Others'
        END AS provider_group,
        SUM(vebal_balance) AS vebal_balance
    FROM ranked_providers
    GROUP BY 1, 2
)

SELECT 
    vebal_round,
    provider_group AS provider,
    vebal_balance,
    vebal_balance / (SELECT SUM(vebal_balance) FROM top_10_and_others 
    WHERE vebal_round = t.vebal_round) AS vebal_pct
FROM top_10_and_others t
WHERE vebal_round > 11
AND vebal_round < (SELECT MAX(vebal_round) FROM query_4001808)
ORDER BY 1 DESC , 3 DESC
