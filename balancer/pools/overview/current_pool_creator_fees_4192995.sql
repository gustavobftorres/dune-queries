-- part of a query repo
-- query name: Current Pool Creator Fees
-- query link: https://dune.com/queries/4192995


WITH fee_setting AS(
SELECT 
    chain,
    pool,
    'Swap' AS fee_type,
    poolCreatorSwapFeePercentage / POWER(10,18) AS fee_percentage
FROM balancer_v3_multichain.protocolfeecontroller_call_setpoolcreatorswapfeepercentage

UNION ALL

SELECT 
    chain,
    pool,
    'Yield' AS fee_type,
    poolCreatorYieldFeePercentage / POWER(10,18) AS fee_percentage
FROM balancer_v3_multichain.protocolfeecontroller_call_setpoolcreatoryieldfeepercentage)

SELECT 
    chain,
    pool,
    fee_type,
    fee_percentage
FROM fee_setting
ORDER BY 1 DESC
