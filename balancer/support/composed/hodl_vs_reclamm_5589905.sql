-- part of a query repo
-- query name: Hodl vs reCLAMM
-- query link: https://dune.com/queries/5589905


SELECT DISTINCT
    bal.day, 
    hodl.current_value_of_investment AS "HODL",
    bal.current_value_of_investment AS "Balancer Pool"
FROM "query_4771209(start='{{start}}', blockchain='{{blockchain}}', pool='{{pool_address}}')" bal
LEFT JOIN "query_4771257(start='{{start}}', blockchain='{{blockchain}}', token_a='{{token_a}}', token_b='{{token_b}}')" hodl
    ON bal.day = hodl.day
ORDER BY bal.day DESC