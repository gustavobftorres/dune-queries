-- part of a query repo
-- query name: Pool Creator Set
-- query link: https://dune.com/queries/4740208


SELECT
    chain,
    pool,
    factory,
    json_extract_scalar(roleAccounts, '$.poolCreator') AS pool_creator
FROM balancer_v3_multichain.vault_evt_poolregistered
ORDER BY 4 DESC