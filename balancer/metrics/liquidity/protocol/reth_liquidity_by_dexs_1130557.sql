-- part of a query repo
-- query name: rETH Liquidity by DEXs
-- query link: https://dune.com/queries/1130557


select day, project, sum(token_amount)
from dex.liquidity
where token_address = '\xae78736Cd615f374D3085123A210448E74Fc6393'
and day >= '2022-01-01'
group by 1, 2