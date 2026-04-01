-- part of a query repo
-- query name: Pool <> Hook Mapping
-- query link: https://dune.com/queries/4761874


WITH pools AS(
SELECT 
    chain, 
    pool, 
    FROM_HEX(JSON_EXTRACT_SCALAR(hooksConfig, '$.hooksContract')) AS hook 
FROM balancer_v3_multichain.vault_evt_poolregistered pools)

SELECT DISTINCT
    p.chain, 
    p.pool,
    p.hook,
    COALESCE(mev.label, surge.label, 'other') AS hook_name
FROM pools p
LEFT JOIN (SELECT chain, 'mev_capture' AS label, contract_address
        FROM balancer_v3_multichain.mevcapturehook_evt_defaultmevtaxmultiplierset) AS mev ON p.hook = mev.contract_address
                                                                                            AND p.chain = mev.chain
LEFT JOIN (SELECT chain, 'stable_surge' AS label, contract_address
        FROM balancer_v3_multichain.stablesurgehook_evt_stablesurgehookregistered) AS surge ON p.hook = surge.contract_address
                                                                                            AND p.chain = surge.chain
WHERE hook != 0x0000000000000000000000000000000000000000