-- part of a query repo
-- query name: Balancer Weekly Volume by Pool Type
-- query link: https://dune.com/queries/3547828


/* Volume per week */
/* Visualization: bar chart */

    SELECT
        DATE_TRUNC('week', block_time) AS week,
        CONCAT('v', version, ': ', pool_type) AS pool_type,
        SUM(amount_usd) AS volume
    FROM balancer.trades t
    WHERE block_time >=   TIMESTAMP '{{1. Start date}}' 
    AND ('{{3. Blockchain}}' = 'All' OR t.blockchain = '{{3. Blockchain}}')
    AND ('{{5. Version}}' = 'All' OR version = '{{5. Version}}')
    GROUP BY 1, 2
