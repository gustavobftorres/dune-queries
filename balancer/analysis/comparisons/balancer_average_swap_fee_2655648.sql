-- part of a query repo
-- query name: Balancer Average Swap Fee
-- query link: https://dune.com/queries/2655648


WITH fees_polygon as
    (SELECT swap_fee_percentage/1e16 AS fee
    FROM balancer_v2_polygon.pools_fees)
 , 
 fees_arbitrum as
    (SELECT swap_fee_percentage/1e16 AS fee
    FROM balancer_v2_arbitrum.pools_fees) 
,    
 fees_ethereum as
    (SELECT swap_fee_percentage/1e16 AS fee
    FROM balancer_v2_ethereum.pools_fees) 
,    
 fees_optimism as
    (SELECT swap_fee_percentage/1e16 AS fee
    FROM balancer_v2_optimism.pools_fees) 

 ,
    fees AS 
        (SELECT * FROM fees_polygon
        UNION
        SELECT * FROM fees_arbitrum
        UNION
        SELECT * FROM fees_ethereum
        UNION
        SELECT * FROM fees_optimism)

SELECT AVG(fee) AS avgfee FROM fees