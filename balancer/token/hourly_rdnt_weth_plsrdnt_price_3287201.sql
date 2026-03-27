-- part of a query repo
-- query name: Hourly RDNT-WETH/plsRDNT Price
-- query link: https://dune.com/queries/3287201


WITH trades AS (
        SELECT
            block_time,
            IF(
                token_bought_address = 0x32df62dc3aed2cd6224193052ce665dc18165841, -- RDNT-WETH BPT
                token_bought_amount / token_sold_amount,
                token_sold_amount / token_bought_amount
            ) AS price
        FROM balancer.trades
        WHERE project_contract_address = 0x451B0Afd69ACe11Ec0AC339033D54d2543b088a8 -- Pool Address
    )
    
-- Takes median price per hour to exclude possible outliers
SELECT
    date_trunc('hour', block_time) AS hour,
    approx_percentile(price, 0.5) AS median_price
FROM trades
GROUP BY 1

-- If you simply want RDNT-WETH/plsRDNT price per Swap
-- SELECT * FROM trades