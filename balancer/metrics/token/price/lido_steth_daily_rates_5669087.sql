-- part of a query repo
-- query name: Lido stETH daily rates
-- query link: https://dune.com/queries/5669087


SELECT 
   evt_block_time AS block_time,
   evt_block_number AS block_number,
   CAST(postTotalEther AS DOUBLE) / CAST(postTotalShares AS DOUBLE) AS rate
FROM lido_ethereum.steth_evt_tokenrebased
ORDER BY 1 DESC
