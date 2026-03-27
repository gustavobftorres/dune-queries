-- part of a query repo
-- query name: aave tokens mapping - gnosis
-- query link: https://dune.com/queries/4307121


WITH aave_tokens AS(
SELECT DISTINCT
    a.aToken, 
    SUBSTRING(a.staticATokenSymbol, 5, 99) AS aTokenSymbol,
    a.contract_address AS staticAToken,
    a.staticATokenName,
    a.staticATokenSymbol,
    t.contract_address AS underlying,
    t.symbol AS underlyingTokenSymbol,
    t.decimals AS underlyingTokenDecimals
FROM aave_v3_gnosis.AToken_evt_Initialized a
JOIN tokens.erc20 t ON LOWER(SUBSTRING(a.staticATokenSymbol, 9, 99)) = LOWER(t.symbol)
WHERE aToken IS NOT NULL
AND blockchain = 'gnosis')

SELECT 
    'gnosis' AS blockchain, 
    * 
FROM aave_tokens