-- part of a query repo
-- query name: veBAL by Top LPs
-- query link: https://dune.com/queries/601405


WITH
    total_vebal AS (
        SELECT day, SUM(vebal_balance) AS total_vebal
        FROM
            balancer_ethereum.vebal_balances_day
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
                   balancer_ethereum.vebal_balances_day
        WHERE
            day = CURRENT_DATE
        ORDER BY
            vebal_balance DESC
        LIMIT 20
    )
SELECT
    r.day,
    r.wallet_address,
    COALESCE(q.provider, t.short_provider, 'Others') AS provider,
    SUM(vebal_balance) AS vebal_balance,
    SUM(vebal_balance) / total_vebal AS pct
FROM  balancer_ethereum.vebal_balances_day AS r
INNER JOIN total_vebal AS v ON v.day = r.day
LEFT JOIN top_providers AS t ON t.wallet_address = r.wallet_address
LEFT JOIN query_3032958 AS q ON q.wallet_address = r.wallet_address
WHERE r.day <= CURRENT_DATE
GROUP BY 1, 2, 3, total_vebal
ORDER BY 1 DESC, 4 DESC