-- part of a query repo
-- query name: Relic holders at block 80293098
-- query link: https://dune.com/queries/3782710


select user_address, sum(balance) from dune.beethovenx.dataset_relics_at_80293098
group by 1
order by 2 desc