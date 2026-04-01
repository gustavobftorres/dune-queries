-- part of a query repo
-- query name: Join/Exit frequency
-- query link: https://dune.com/queries/1431796


select date_trunc('month', evt_block_time) as day, count(1) 
from balancer_v2."Vault_evt_Swap"
where ("tokenIn" = SUBSTRING("poolId" FOR 20)
OR "tokenOut" = SUBSTRING("poolId" FOR 20))
group by 1