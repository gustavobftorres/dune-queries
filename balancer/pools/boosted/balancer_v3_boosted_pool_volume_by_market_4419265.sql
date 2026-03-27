-- part of a query repo
-- query name: Balancer V3 Boosted Pool Volume by Market
-- query link: https://dune.com/queries/4419265



SELECT
    CASE 
        WHEN '{{aggregation}}' = 'daily' THEN block_date
        WHEN '{{aggregation}}' = 'weekly' THEN DATE_TRUNC('week', block_date)
        WHEN '{{aggregation}}' = 'monthly' THEN DATE_TRUNC('month', block_date)
    END AS week,
        lending_market,
        SUM(amount_usd) AS volume
    FROM
        balancer.trades s
    INNER JOIN query_4419172 m ON s.project_contract_address = m.address
    AND s.blockchain = m.blockchain
        WHERE 1 = 1
        AND ('{{blockchain}}' = 'All' OR s.blockchain = '{{blockchain}}')
        AND version = '3'
    GROUP BY 1, 2