-- part of a query repo
-- query name: Growth of 10k
-- query link: https://dune.com/queries/4529436


select DISTINCT
    bal.day, 
    hodl.current_value_of_investment as "HODL",
    bal.current_value_of_investment as "Balancer",
    uni.current_value_of_investment as "Uni v2",
    ((bal.current_value_of_investment/uni.current_value_of_investment)-1)*100 as over_uni_return,
    ((bal.current_value_of_investment/hodl.current_value_of_investment)-1)*100 as over_hodl_return
from "query_4771209(start='{{start}}', blockchain='{{blockchain}}', pool='{{balancer pool}}')" bal
left join "query_4771257(start='{{start}}', token_a='{{token 1}}', token_b='{{token 2}}')" hodl
on bal.day=hodl.day
left join "query_4771259(start='{{start}}', blockchain='{{blockchain}}', pool='{{uniswap pool}}', token_a='{{token 1}}', token_b='{{token 2}}')" uni
on bal.day=uni.day
order by bal.day desc
