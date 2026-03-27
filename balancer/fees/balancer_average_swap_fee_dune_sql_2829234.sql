-- part of a query repo
-- query name: Balancer Average Swap Fee (Dune SQL)
-- query link: https://dune.com/queries/2829234


WITH fees_polygon as
    (SELECT swap_fee_percentage/1e16 AS fee , 'polygon' as blockchain
    FROM balancer_v2_polygon.pools_fees
    )
 , 
 fees_arbitrum as
    (SELECT swap_fee_percentage/1e16  AS fee, 'arbitrum' as blockchain
    FROM balancer_v2_arbitrum.pools_fees
    ) 
,    
 fees_ethereum as
    (SELECT swap_fee_percentage/1e16 AS fee, 'ethereum' as blockchain 
    FROM balancer_v2_ethereum.pools_fees
    ) 
,    
 fees_optimism as
    (SELECT swap_fee_percentage/1e16 AS fee, 'optimism' as blockchain 
    FROM balancer_v2_optimism.pools_fees
    ) 
,    
 fees_gnosis as
    (SELECT swap_fee_percentage/1e16 AS fee, 'gnosis' as blockchain 
    FROM balancer_v2_gnosis.pools_fees
    ),
    
 fees_avalanche_c as
    (SELECT swap_fee_percentage/1e16 AS fee, 'avalanche_c' as blockchain 
    FROM balancer_v2_avalanche_c.pools_fees
    ),
    
 fees_base as
    (SELECT swap_fee_percentage/1e16 AS fee, 'base' as blockchain 
    FROM balancer_v2_base.pools_fees
    ) 

 ,
    fees AS 
        (SELECT * FROM fees_polygon
        UNION
        SELECT * FROM fees_arbitrum
        UNION
        SELECT * FROM fees_ethereum
        UNION
        SELECT * FROM fees_optimism
        UNION
        SELECT * FROM fees_gnosis
        UNION
        SELECT * FROM fees_avalanche_c
        UNION
        SELECT * FROM fees_base) 
        
SELECT AVG(fee) AS avgfee FROM fees
WHERE '{{4. Blockchain}}' = 'All' OR blockchain = '{{4. Blockchain}}'