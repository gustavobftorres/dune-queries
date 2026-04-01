-- part of a query repo
-- query name: Buffer Selected
-- query link: https://dune.com/queries/4452435


SELECT DISTINCT
    blockchain,
    symbol
FROM query_4144874 q
WHERE q.wrappedToken = {{wrapped_token}}
AND q.blockchain = '{{blockchain}}'