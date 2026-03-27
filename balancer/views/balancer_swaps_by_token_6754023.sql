-- part of a query repo
-- query name: Balancer Swaps by Token
-- query link: https://dune.com/queries/6754023


-- Materialized view: balancer_swaps_by_token
SELECT 
    block_time,
    t.symbol AS token,
    d.blockchain,
    d.version,
    amount_usd
FROM balancer.trades d
LEFT JOIN tokens.erc20 t 
    ON d.token_bought_address = t.contract_address 
    AND d.blockchain = t.blockchain