-- part of a query repo
-- query name: veBAL by Top LPs and Future Unlocks
-- query link: https://dune.com/queries/3645067


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
    )
SELECT
    r.day,
    COALESCE(q.provider, t.short_provider, 'Others') AS provider,
    SUM(vebal_balance) AS vebal_balance,
    SUM(vebal_balance) / total_vebal AS pct
FROM  query_3644589 AS r
INNER JOIN total_vebal AS v ON v.day = r.day
LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
LEFT JOIN query_3032958 AS q ON q.wallet_address = r.wallet_address
WHERE r.day >= TIMESTAMP '{{1. Start Date}}'
AND r.day <= TIMESTAMP '{{2. End Date}}'
AND ('{{Provider}}' = 'All' OR '{{Provider}}' = q.provider)
GROUP BY 1, 2, total_vebal
ORDER BY 1 DESC, 3 DESC