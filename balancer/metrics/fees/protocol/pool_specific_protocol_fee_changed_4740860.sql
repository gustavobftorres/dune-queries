-- part of a query repo
-- query name: Pool specific protocol fee changed
-- query link: https://dune.com/queries/4740860


WITH fee_setting AS(
SELECT 
    chain,
    pool,
    'Swap' AS fee_type,
    newProtocolSwapFeePercentage AS fee
FROM balancer_v3_multichain.protocolfeecontroller_call_setprotocolswapfeepercentage

UNION ALL

SELECT 
    chain,
    pool,
    'Yield' AS fee_type,
    newProtocolYieldFeePercentage AS fee
FROM balancer_v3_multichain.protocolfeecontroller_call_setprotocolyieldfeepercentage)

SELECT 
    chain,
    pool,
    fee_type,
    fee
FROM fee_setting
ORDER BY 1 DESC
