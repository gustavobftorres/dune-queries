-- part of a query repo
-- query name: erc4626_tokens
-- query link: https://dune.com/queries/4717791


SELECT 'All'
UNION 
SELECT DISTINCT erc4626_token_symbol
FROM query_4549390
ORDER BY 1 ASC