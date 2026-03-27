-- part of a query repo
-- query name: Growth of 10k - cbBTC/WETH (Base)
-- query link: https://dune.com/queries/5652571


SELECT DISTINCT
    bal.day, 
    hodl.current_value_of_investment AS "HODL",
    bal.current_value_of_investment AS "Balancer",
    aero.current_value_of_investment as "Aero"
FROM "query_5652148(start='{{start}}', blockchain='{{blockchain}}', pool='{{balancer_pool}}')" bal
LEFT JOIN "query_4771257(start='{{start}}', blockchain='{{blockchain}}', token_a='{{token_a}}', token_b='{{token_b}}')" hodl
    ON bal.day = hodl.day
LEFT JOIN "query_5651828(start='{{start}}', blockchain='{{blockchain}}', pool='{{aero_pool}}')" aero
    ON bal.day=aero.day
ORDER BY bal.day DESC
