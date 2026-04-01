-- part of a query repo
-- query name: Protocol Fee Collected
-- query link: https://dune.com/queries/3293596


WITH bpt_prices AS (
        SELECT
            CAST(day AS DATE) AS day,
            blockchain,
            token AS contract_address,
            CAST(price AS DOUBLE) AS price
        FROM dune.balancer.result_bpt_prices
        WHERE is_finite(price) -- removes 2 outliers
    ),
    
    protocol_fee AS (
        SELECT
            f.*,
            COALESCE(token_amount_raw * price / 1e18, protocol_fee_collected_usd) AS amount_usd
        FROM balancer.protocol_fee f
        LEFT JOIN bpt_prices b
        ON b.blockchain = f.blockchain
        AND b.contract_address = f.token_address
        AND b.day = f.day
    )

SELECT 
    blockchain,
    pool_symbol,
    pool_address,
    sum(amount_usd) as protocol_fee_collected
FROM protocol_fee
WHERE day > TIMESTAMP '{{1. Start Date}}'
AND day <= TIMESTAMP '{{2. End Date}}'
AND '{{3. Blockchain}}' = 'All' OR blockchain = '{{3. Blockchain}}'
GROUP BY 1, 2, 3
ORDER BY 4 DESC NULLS LAST
