-- part of a query repo
-- query name: LZ Humpy interaction addresses
-- query link: https://dune.com/queries/3782924


SELECT a.*, blockchain, provider FROM query_3779027 a
INNER JOIN query_3032958 q ON a.user_address = q.wallet_address AND a.blockchain = 'ethereum'
AND q.provider = 'Humpy'