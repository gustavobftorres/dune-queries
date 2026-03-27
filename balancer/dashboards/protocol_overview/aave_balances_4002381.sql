-- part of a query repo
-- query name: AAVE Balances
-- query link: https://dune.com/queries/4002381


SELECT t.*, l.owner_key FROM tokens_ethereum.balances_daily t
LEFT JOIN labels.owner_addresses l ON l.address = t.address
AND l.blockchain = 'ethereum'
WHERE token_address = 0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9
AND t.day = CURRENT_DATE
AND balance_usd > 1e3
ORDER BY balance_usd DESC