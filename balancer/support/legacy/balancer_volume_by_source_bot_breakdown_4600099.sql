-- part of a query repo
-- query name: Balancer Volume by Source - Bot Breakdown
-- query link: https://dune.com/queries/4600099


       SELECT 
            t.blockchain,
            t.tx_to AS channel,
            SUM(CASE WHEN project = 'balancer' THEN amount_usd END) AS balancer_volume,
            SUM(CASE WHEN project = '{{dex_2}}' THEN amount_usd END) AS {{dex_2}}_volume,
            SUM(CASE WHEN project = '{{dex_3}}' THEN amount_usd END) AS {{dex_3}}_volume,
            COUNT(CASE WHEN project = 'balancer' THEN tx_hash END) AS balancer_txns,
            COUNT(CASE WHEN project = '{{dex_2}}' THEN tx_hash END) AS {{dex_2}}_txns,
            COUNT(CASE WHEN project = '{{dex_3}}' THEN tx_hash END) AS {{dex_3}}_txns
        FROM dex.trades t
        JOIN dune.balancer.result_balancer_volume_source_classifier c
        ON c.blockchain = t.blockchain AND c.channel = tx_to
        AND c.class = 'MEV Bot'
        WHERE amount_usd IS NOT NULL
        AND block_date >= TIMESTAMP '{{start_date}}'
        AND t.blockchain IN ({{blockchain}})
        AND (CASE WHEN project = 'balancer' THEN '{{balancer_token_pair}}' = 'All' OR token_pair = '{{balancer_token_pair}}'
            WHEN project != 'balancer' THEN '{{other_dexs_token_pair}}' = 'All' OR token_pair = '{{other_dexs_token_pair}}'
            END)
        GROUP BY 1, 2
        ORDER BY 3 DESC
