-- part of a query repo
-- query name: Balancer Volume by Time Unit
-- query link: https://dune.com/queries/3268220


SELECT
    DATE_TRUNC('{{Time Unit}}', block_date) AS day,
    SUM(amount_usd) AS volume
FROM
    balancer.trades
WHERE
    CASE 
        WHEN '{{Time Unit}}' = 'DAY' THEN block_date >= current_date - interval '{{Date Range in Time Units}}' DAY
        WHEN '{{Time Unit}}' = 'WEEK' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' DAY) * 7
        WHEN '{{Time Unit}}' = 'MONTH' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - interval '{{Date Range in Time Units}}' MONTH
        WHEN '{{Time Unit}}' = 'QUARTER' THEN block_date >= date_trunc('{{Time Unit}}', current_date) - (interval '{{Date Range in Time Units}}' MONTH * 3)
    END
GROUP BY 1