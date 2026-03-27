-- part of a query repo
-- query name: Pool Data - WIP
-- query link: https://dune.com/queries/3608718


SELECT 
    l.day,
    l.blockchain,
    l.pool_id,
    sum(l.protocol_liquidity_usd) AS tvl/*,
    sum(f.protocol_fee_collected_usd) AS fees*//*,
    sum(l.protocol_liquidity_usd / s.supply) AS bpt_price*/
FROM balancer.liquidity l
/*LEFT JOIN balancer.protocol_fee f ON l.day = f.day
AND l.pool_id = f.pool_id
AND l.blockchain = f.blockchain*/
/*LEFT JOIN balancer.bpt_supply s ON l.day = s.day
AND l.pool_address = s.token_address
AND l.blockchain = s.blockchain*/
WHERE l.day = current_date - interval '1' day
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 4 DESC
    