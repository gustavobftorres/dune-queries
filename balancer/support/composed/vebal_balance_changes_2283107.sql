-- part of a query repo
-- query name: veBAL Balance Changes
-- query link: https://dune.com/queries/2283107


WITH balances_today AS (
        SELECT *
        --vebal_balances_day
        FROM query_2276840
        WHERE day = date_trunc('day', now())
    ),
    balances_1d_ago AS (
        SELECT *
        --vebal_balances_day
        FROM query_2276840
        WHERE day =  date_add('day', -1, date_trunc('day', now()))
    ),
    balances_7d_ago AS (
        SELECT *
        --vebal_balances_day
        FROM query_2276840
        WHERE day = date_add('day', -7, date_trunc('day', now()))
    )
SELECT 
    CONCAT(
        '<a href="https://duneanalytics.com/balancerlabs/veBAL-Analysis?1.%20Provider=0x', 
        LOWER(to_hex(a.wallet_address)), 
        '">0x',
        LOWER(to_hex(a.wallet_address)), 
        '↗</a>') as provider, 
    (a.vebal_balance - COALESCE(b.vebal_balance, 0)) as "1d_change",
    (a.vebal_balance - COALESCE(c.vebal_balance, 0)) as "7d_change"
FROM balances_today AS a
LEFT JOIN balances_1d_ago AS b
on a.wallet_address = b.wallet_address
LEFT JOIN balances_7d_ago AS c
on a.wallet_address = c.wallet_address
ORDER BY 2 DESC
