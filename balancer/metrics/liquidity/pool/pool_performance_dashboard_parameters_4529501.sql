-- part of a query repo
-- query name: Pool Performance Dashboard Parameters
-- query link: https://dune.com/queries/4529501


SELECT 
    l.name AS b_pool_selected,
    '{{blockchain}}' AS blockchain,
    t1.symbol AS symbol1,
    t2.symbol AS symbol2
FROM labels.addresses l
CROSS JOIN tokens.erc20 t1
CROSS JOIN tokens.erc20 t2
WHERE l.category IN ('balancer_v1_pool', 'balancer_v2_pool','balancer_v3_pool')
AND l.blockchain = '{{blockchain}}'
AND t1.blockchain = '{{blockchain}}'
AND t2.blockchain = '{{blockchain}}'
AND l.address = {{balancer pool}}
AND t1.contract_address ={{token 1}}
AND t2.contract_address = {{token 2}}
