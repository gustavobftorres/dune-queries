-- part of a query repo
-- query name: Balancer LP Weekly Revenues
-- query link: https://dune.com/queries/67669


SELECT 
    date_trunc('week', block_time) AS week, 
    CONCAT('V', version) AS version,
    SUM(usd_amount) AS volume,
    SUM(usd_amount * swap_fee) AS revenue
FROM balancer.view_trades t
GROUP BY 1, 2
ORDER BY 1