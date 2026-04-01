-- part of a query repo
-- query name: Balancer TVL
-- query link: https://dune.com/queries/3142123


SELECT CASE WHEN '{{Currency}}' = 'USD' 
        THEN concat('$', format_number(sum(protocol_liquidity_usd))) 
        WHEN '{{Currency}}' = 'eth' 
        THEN format_number(sum(protocol_liquidity_eth))
        END AS tvl 
FROM balancer.liquidity x
INNER JOIN (SELECT max(day) AS latest_date FROM balancer.liquidity WHERE version = '2') y
    ON y.latest_date = x.day