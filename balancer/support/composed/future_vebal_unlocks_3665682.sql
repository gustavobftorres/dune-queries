-- part of a query repo
-- query name: Future veBAL Unlocks
-- query link: https://dune.com/queries/3665682


WITH
    total_vebal AS (
        SELECT day, SUM(vebal_balance) AS total_vebal
        FROM
            query_3644589
        GROUP BY 1
    ),
    
    top_providers AS (
        SELECT
            wallet_address,
            LOWER(
                CONCAT(
                    '0x',
                    SUBSTRING(to_hex(wallet_address), 1, 4),
                    '...',
                    SUBSTRING(to_hex(wallet_address), 37, 4)
                )
            ) AS short_provider
        FROM
            --vebal_balances_day
                   query_3644589
        WHERE
            day = CURRENT_DATE
        ORDER BY
            vebal_balance DESC
        LIMIT 20
    ),
    
total_provider_balance AS(
SELECT
    r.day AS day,
    COALESCE(q.provider, t.short_provider, 'Others') AS provider,
    SUM(r.vebal_balance) AS provider_balance
FROM query_3644589 AS r
LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
LEFT JOIN query_3032958 AS q ON q.wallet_address = r.wallet_address
GROUP BY 1, 2)

SELECT
    DATE_TRUNC('week', FROM_unixtime(unlocked_at)) AS unlocked_at,
    COALESCE(q.provider, t.short_provider, 'Others') AS provider,
    SUM(r.vebal_balance) AS unlocked_balance,
    SUM(r.vebal_balance/v.total_vebal) AS unlocked_total_relative_balance,
    SUM(r.vebal_balance/b.provider_balance) AS unlocked_relative_balance
FROM query_3644589 AS r
LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
LEFT JOIN query_3032958 AS q ON q.wallet_address = r.wallet_address
LEFT JOIN total_vebal AS v ON v.day = r.day
LEFT JOIN total_provider_balance AS b ON b.day = r.day AND b.provider = COALESCE(q.provider, t.short_provider, 'Others')
WHERE DATE_TRUNC('week', FROM_unixtime(unlocked_at)) >= TIMESTAMP '{{1. Start Date}}'
AND DATE_TRUNC('week', FROM_unixtime(unlocked_at)) <= TIMESTAMP '{{2. End Date}}'
AND ('{{Provider}}' = 'All' OR '{{Provider}}' = q.provider)
AND r.day = FROM_unixtime(unlocked_at) - interval '2' day
GROUP BY 1, 2
ORDER BY 1 DESC