-- part of a query repo
-- query name: Balancer Liquidity Monitoring
-- query link: https://dune.com/queries/3710971


--After running on monday, retrieve csv and compare to previous monday, to see if any breaking changes happened, considering that the current results have been tested and are OK, despite being flagged
--https://colab.research.google.com/drive/1qri_N4fODXggvFDgs8ItFyjxXZhKMu87#scrollTo=tTJIE_oZq8z0

WITH liquidity_changes AS (
    SELECT 
        l1.day AS day_1,
        l1.pool_id AS pool_id,
        l1.pool_symbol,
        l1.blockchain,
        l1.pool_type,
        SUM(l1.protocol_liquidity_usd) AS liquidity_1,
        l2.day AS day_2,
        SUM(l2.protocol_liquidity_usd) AS liquidity_2
    FROM 
        balancer.liquidity l1
    JOIN 
        balancer.liquidity l2 ON l1.pool_id = l2.pool_id AND l1.blockchain = l2.blockchain
        AND l2.day = DATE_ADD('day', 1, l1.day) -- Joining on consecutive days
    GROUP BY 1, 2, 3, 4, 5, 7
)
SELECT 
    lc.day_1,
    lc.day_2,
    lc.pool_id,
    lc.pool_symbol,
    lc.blockchain,
    lc.pool_type,
    lc.liquidity_1 AS liquidity_day_1,
    lc.liquidity_2 AS liquidity_day_2
FROM 
    liquidity_changes lc
WHERE 
    lc.liquidity_2 > 50 * lc.liquidity_1
AND lc.liquidity_1 > 1e5
AND lc.liquidity_2 > 1e7
AND CAST(lc.day_1 AS VARCHAR)||CAST(lc.pool_id AS VARCHAR) NOT IN (SELECT CAST(day_1 AS VARCHAR)
||CAST(pool_id AS VARCHAR) FROM dune.balancer.dataset_liquidity_outlier_cleanup)
AND lc.pool_id != 0x70ff0078d55ce9c1a0e668f35eb4400a4300122d000000000000000000000beb
UNION ALL

SELECT NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL
ORDER BY 1 DESC
