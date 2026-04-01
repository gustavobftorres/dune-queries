-- part of a query repo
-- query name: veBAL / vlAURA - Delegation Map
-- query link: https://dune.com/queries/6672392


SELECT
    a.provider AS delegator_label,
    CONCAT('0x', to_hex(a._user)) AS delegator_wallet,
    CONCAT('0x', to_hex(a.delegate)) AS delegate_wallet,
    COALESCE(q.provider, CONCAT('0x', SUBSTRING(to_hex(a.delegate), 1, 8), '...')) AS delegate_label,
    CASE 
        WHEN a._user = a.delegate THEN 'self'
        ELSE 'delegated'
    END AS delegation_type,
    SUM(a.total_locked) AS locked_vlaura,
    SUM(a.vebal_balance) AS vebal_equivalent
FROM query_4023187 a
LEFT JOIN query_4007145 q ON a.delegate = q.wallet_address
WHERE a.vebal_round = (SELECT MAX(vebal_round) - 1 FROM query_4023187)
  AND a.total_locked > 0
GROUP BY 1, 2, 3, 4, 5
ORDER BY 7 DESC