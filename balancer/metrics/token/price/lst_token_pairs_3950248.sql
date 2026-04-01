-- part of a query repo
-- query name: LST Token Pairs
-- query link: https://dune.com/queries/3950248


WITH pairs AS(
SELECT 
        project, 
        token_pair, 
        SUM(amount_usd) AS volume
    FROM dex.trades t
    INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
    AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE project =  'balancer'
    GROUP BY 1, 2
    ORDER BY 3 DESC)
    
    SELECT 'All', 'All', 1e16
    UNION ALL
    SELECT * FROM pairs
    ORDER BY 3 DESC