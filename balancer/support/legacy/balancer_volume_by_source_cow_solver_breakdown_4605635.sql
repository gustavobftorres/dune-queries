-- part of a query repo
-- query name: Balancer Volume by Source - CoW Solver Breakdown
-- query link: https://dune.com/queries/4605635


WITH solvers AS(
    SELECT
        'ethereum' AS blockchain,
        address,
        name
    FROM cow_protocol_ethereum.solvers
    
    UNION 
    
    SELECT
        'gnosis' AS blockchain,
        address,
        name
    FROM cow_protocol_gnosis.solvers

    UNION 
    
    SELECT
        'arbitrum' AS blockchain,
        address,
        name
    FROM cow_protocol_arbitrum.solvers    

    UNION 
    
    SELECT
        'base' AS blockchain,
        address,
        name
    FROM cow_protocol_base.solvers       
)


        SELECT 
            t.blockchain,
            c.name AS solver,
            SUM(CASE WHEN project = 'balancer' THEN amount_usd END) AS balancer_volume,
            SUM(CASE WHEN project = '{{dex_2}}' THEN amount_usd END) AS {{dex_2}}_volume,
            SUM(CASE WHEN project = '{{dex_3}}' THEN amount_usd END) AS {{dex_3}}_volume,
            COUNT(CASE WHEN project = 'balancer' THEN tx_hash END) AS balancer_txns,
            COUNT(CASE WHEN project = '{{dex_2}}' THEN tx_hash END) AS {{dex_2}}_txns,
            COUNT(CASE WHEN project = '{{dex_3}}' THEN tx_hash END) AS {{dex_3}}_txns
        FROM dex.trades t
        JOIN solvers c
        ON c.blockchain = t.blockchain AND c.address = tx_from
        WHERE amount_usd IS NOT NULL
        AND tx_to = 0x9008d19f58aabd9ed0d60971565aa8510560ab41
        AND block_date >= TIMESTAMP '{{start_date}}'
        AND t.blockchain IN ({{blockchain}})
        AND (CASE WHEN project = 'balancer' THEN '{{balancer_token_pair}}' = 'All' OR token_pair = '{{balancer_token_pair}}'
            WHEN project != 'balancer' THEN '{{other_dexs_token_pair}}' = 'All' OR token_pair = '{{other_dexs_token_pair}}'
            END)        
        GROUP BY 1, 2
        ORDER BY 3 DESC
