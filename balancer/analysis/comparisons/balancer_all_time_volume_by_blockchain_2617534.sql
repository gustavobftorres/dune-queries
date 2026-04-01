-- part of a query repo
-- query name: Balancer all-time volume by blockchain
-- query link: https://dune.com/queries/2617534


   SELECT sum(amount_usd) as amount_usd, blockchain
        FROM dex.trades d
        WHERE project = 'balancer'
        GROUP BY 2
        ORDER BY 1 ASC