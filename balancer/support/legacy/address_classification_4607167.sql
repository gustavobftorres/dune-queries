-- part of a query repo
-- query name: Address Classification
-- query link: https://dune.com/queries/4607167


SELECT
    l.channel,
    t.blockchain,
    l.class,
    SUM(amount_usd) AS volume
FROM balancer.trades t
JOIN dune.balancer.result_balancer_volume_source_classifier l
ON t.blockchain = l.blockchain AND t.tx_to = l.channel
GROUP BY 1, 2, 3
ORDER BY 4 DESC