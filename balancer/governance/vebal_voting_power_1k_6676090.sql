-- part of a query repo
-- query name: veBAL Voting Power (>1k)
-- query link: https://dune.com/queries/6676090


WITH vebal_balances AS (
    SELECT
        wallet_address,
        vebal_balance
    FROM balancer_ethereum.vebal_balances_day
    WHERE day = CURRENT_DATE
    AND vebal_balance > 1000
),

total_vebal AS (
    SELECT SUM(vebal_balance) as total FROM vebal_balances
)

SELECT
    vb.wallet_address,
    COALESCE(
        vebal.provider,
        sh.label,
        ens.name,
        CONCAT('0x', "LEFT"(TO_HEX(vb.wallet_address), 4), '...', "RIGHT"(TO_HEX(vb.wallet_address), 4))
    ) as wallet_label,
    vb.vebal_balance as vebal_power,
    (vb.vebal_balance / tv.total) as vebal_pct
FROM vebal_balances vb
CROSS JOIN total_vebal tv
LEFT JOIN query_3032958 vebal ON vebal.wallet_address = vb.wallet_address
LEFT JOIN query_6675327 sh ON sh.address = vb.wallet_address
LEFT JOIN labels.ens ens ON ens.address = vb.wallet_address AND ens.blockchain = 'ethereum'
ORDER BY vebal_power DESC
