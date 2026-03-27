-- part of a query repo
-- query name: addresses_labels
-- query link: https://dune.com/queries/2998487


SELECT l.* FROM labels.all l
LEFT JOIN query_2478528 q ON q.address = CAST(l.address as varchar) AND q.blockchain = l.blockchain
WHERE q.blockchain = 'ethereum' AND l.category = 'contracts' AND q.name = 'Arbitrage Bot'