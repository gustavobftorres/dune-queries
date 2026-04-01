-- part of a query repo
-- query name: aave tokens mapping - gnosis
-- query link: https://dune.com/queries/4301939


WITH aave_tokens AS(
SELECT 
    a.aToken,
    c.aTokenSymbol,
    b.staticAToken,
    a.staticATokenName,
    a.staticATokenSymbol,
    b.underlying,
    t.symbol,
    t.decimals
FROM aave_ethereum.StaticATokenLM_evt_Initialized a
JOIN aave_ethereum.StaticATokenFactory_evt_StaticTokenCreated b
ON b.staticAToken = a.contract_address
JOIN aave_v3_ethereum.VariableDebtToken_evt_Initialized c
ON a.aToken = c.contract_address
JOIN tokens.erc20 t
ON t.blockchain = 'ethereum'
AND b.underlying = t.contract_address)

SELECT * FROM aave_tokens