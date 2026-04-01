-- part of a query repo
-- query name: Current veBAL balance and lock time
-- query link: https://dune.com/queries/3698929


WITH
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
    lock_time / (365 * 86400 / 12) AS lock_months,
    r.wallet_address,
    COALESCE(q.provider, t.short_provider, 'Others') AS provider,
    SUM(r.vebal_balance) AS vebal_balance
FROM query_3644589 AS r
LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
LEFT JOIN query_3032958 AS q ON q.wallet_address = r.wallet_address
WHERE day = CURRENT_DATE
AND ('{{Provider}}' = 'All' OR '{{Provider}}' = q.provider)
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC