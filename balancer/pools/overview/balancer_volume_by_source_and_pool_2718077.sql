-- part of a query repo
-- query name: Balancer Volume by Source and Pool
-- query link: https://dune.com/queries/2718077


WITH 
    raw_swaps AS (
        SELECT 
            date_trunc('week', block_time) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            sum(amount_usd) AS volume
        FROM balancer.trades
        WHERE amount_usd IS NOT NULL
        AND block_time >= TIMESTAMP '{{2. Start date}}'
        AND block_time <= TIMESTAMP '{{3. End date}}'
        AND ('{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}')
        AND ('{{1. Pool ID}}' = 'All' OR project_contract_address = BYTEARRAY_SUBSTRING({{1. Pool ID}},1,20))
        GROUP BY 1, 2, 3
    )

SELECT
    s.week,
    c.class,
    SUM(s.volume) AS volume
FROM raw_swaps s
INNER JOIN dune.balancer.result_balancer_volume_source_classifier c
  ON s.channel = c.channel AND s.blockchain = c.blockchain
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

