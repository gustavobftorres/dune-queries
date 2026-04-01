-- part of a query repo
-- query name: Balancer Average Swap Fee (Pools Overview)
-- query link: https://dune.com/queries/2872977


SELECT AVG(swap_fee_percentage) / 1e16 AS avgfee FROM balancer.pools_fees