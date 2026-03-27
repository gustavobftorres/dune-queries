-- part of a query repo
-- query name: all balancer reclamms
-- query link: https://dune.com/queries/5057796


SELECT
    chain,
    pool
FROM balancer_v3_multichain.vault_evt_poolregistered
WHERE 1=1
AND chain IN (
    'base',
    'ethereum'
)
AND factory IN (
    0x84813aa3e079a665c0b80f944427ee83cba63617,
    0x7fA49Df302a98223d98D115fc4FCD275576f6faA
)
