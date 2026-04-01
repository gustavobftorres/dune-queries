-- part of a query repo
-- query name: BEETS bridged from and to OPTIMISM - addresses
-- query link: https://dune.com/queries/3781662


SELECT user_address, sum(amount_original) AS beets_bridged
FROM query_3781645 q
-- RIGHT JOIN dune.beethovenx.dataset_user_address_relics_80293098csv r on r.useraddress = q.user_address
GROUP BY 1
ORDER BY 2 DESC