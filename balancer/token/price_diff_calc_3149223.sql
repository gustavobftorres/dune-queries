-- part of a query repo
-- query name: price diff calc
-- query link: https://dune.com/queries/3149223


WITH a AS (
    SELECT sum(protocol_liquidity_usd) AS tvl 
    FROM balancer.liquidity x
    INNER JOIN (SELECT max(day) AS latest_date FROM balancer.liquidity) y
        ON y.latest_date = x.day
    WHERE pool_id = {{pool_id}}
        AND blockchain = '{{blockchain}}'
)
SELECT 
    tvl
    , tvl / (CAST('{{actual supply 1}}' AS uint256) / 1e18) AS bpt_usd_1 
    , tvl / (CAST('{{actual supply 2}}' AS uint256) / 1e18) AS bpt_usd_2
    , abs(
        (tvl / (CAST('{{actual supply 1}}' AS uint256) / 1e18)) - (tvl / (CAST('{{actual supply 2}}' AS uint256) / 1e18))
    ) / (tvl / (CAST('{{actual supply 1}}' AS uint256) / 1e18)) AS diff
FROM a