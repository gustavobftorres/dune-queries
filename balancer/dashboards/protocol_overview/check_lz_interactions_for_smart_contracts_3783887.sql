-- part of a query repo
-- query name: Check LZ interactions for smart contracts
-- query link: https://dune.com/queries/3783887


WITH responses AS (
    SELECT
        token,
        http_get(
            CONCAT('https://api.etherscan.io/api?module=token&action=tokeninfo&contractaddress=', CAST(token AS VARCHAR),
            ' &apikey=SZYU3WWIQTZDKQB9ZNREVTTUTQH6W47SY8')
        ) AS response
    FROM query_3500282
    WHERE blockchain = 'ethereum'
)
SELECT * FROM responses
/*SELECT 
    token,
    JSON_EXTRACT_SCALAR(response, '$.status') AS status,
    JSON_EXTRACT_SCALAR(response, '$.result[0].contractAddress') AS smart_contract_address,
    CASE
        WHEN JSON_EXTRACT_SCALAR(response, '$.status') = '1' THEN 'Smart Contract'
        ELSE 'EOA'
    END AS address_type
FROM responses*/

