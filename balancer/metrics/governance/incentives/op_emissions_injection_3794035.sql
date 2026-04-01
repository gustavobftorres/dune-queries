-- part of a query repo
-- query name: OP Emissions Injection
-- query link: https://dune.com/queries/3794035


SELECT e.* 
FROM balancer_optimism.ChildChainGaugeInjector_evt_EmissionsInjection e
LEFT JOIN tokens_optimism.transfers t ON t.tx_from = e.gauge