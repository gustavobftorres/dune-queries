-- part of a query repo
-- query name: cow_amm_base_table
-- query link: https://dune.com/queries/3957949


SELECT * FROM dune.balancer.result_b_cow_amm_base_table_ethereum
UNION ALL
SELECT * FROM dune.balancer.result_b_cow_amm_base_table_gnosis
UNION ALL
SELECT * FROM dune.balancer.result_b_cow_amm_base_table_arbitrum
UNION ALL
SELECT * FROM dune.balancer.result_b_cow_amm_base_table_base