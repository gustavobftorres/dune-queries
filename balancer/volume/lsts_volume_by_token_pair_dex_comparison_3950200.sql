-- part of a query repo
-- query name: LSTs Volume by Token Pair - DEX Comparison
-- query link: https://dune.com/queries/3950200


    SELECT 
        project, 
        token_pair, 
        SUM(amount_usd) AS volume,
        SUM(t.amount_usd) FILTER (WHERE t.block_time >= NOW() - INTERVAL '7' DAY) AS volume_7d,
        SUM(t.amount_usd) FILTER(WHERE t.block_time >= NOW() - INTERVAL '30' DAY) AS volume_30d
    FROM dex.trades t
    INNER JOIN dune.balancer.result_lst_tokens l ON t.blockchain = l.blockchain 
    AND (t.token_bought_address = l.contract_address OR t.token_sold_address = l.contract_address)
    WHERE t.block_date >= TIMESTAMP '{{Start date}}'
    AND t.block_date <= TIMESTAMP '{{End date}}'
    AND ('{{Blockchain}}' = 'All' OR t.blockchain = '{{Blockchain}}')
    AND token_pair = '{{Token Pair}}'
    GROUP BY 1, 2
    ORDER BY 4 DESC;
