-- part of a query repo
-- query name: Gnosis Safes Ownership for markusbkoch.eth
-- query link: https://dune.com/queries/3464838


-- SELECT
--   s.address,
--   s.blockchain,
--   DATE_TRUNC('day', s.creation_time) AS creation_time,
--   s.creation_version
-- FROM safe.safes AS s
-- JOIN ens.resolver_latest AS e
--   ON s.address = e.address
-- WHERE
--   e.name = 'markusbkoch.eth'
  
SELECT
  *
FROM safe.safes
limit 10