-- part of a query repo
-- query name: test
-- query link: https://dune.com/queries/190850


select * from dex.trades where project = 'Quickswap' order by usd_amount desc nulls last limit 100