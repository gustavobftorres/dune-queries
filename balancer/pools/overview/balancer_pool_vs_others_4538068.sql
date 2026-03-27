-- part of a query repo
-- query name: Balancer Pool vs. Others
-- query link: https://dune.com/queries/4538068


WITH prep AS(
select 
    bal.day, 
    hodl.current_value_of_investment AS hodl_current_value_of_investment,
    bal.current_value_of_investment AS bal_current_value_of_investment,
    uni.current_value_of_investment AS uni_current_value_of_investment
from "query_4771209(start='{{start}}', blockchain='{{blockchain}}', pool='{{balancer pool}}')" bal
left join "query_4771257(start='{{start}}', token_a='{{token 1}}', token_b='{{token 2}}')" hodl
on bal.day=hodl.day
left join "query_4771259(start='{{start}}', blockchain='{{blockchain}}', pool='{{uniswap pool}}')" uni
on bal.day=uni.day
order by bal.day desc)

SELECT
    ((bal_current_value_of_investment/uni_current_value_of_investment)-1)*100 as over_uni_return,
    ((bal_current_value_of_investment/hodl_current_value_of_investment)-1)*100 as over_hodl_return
FROM prep