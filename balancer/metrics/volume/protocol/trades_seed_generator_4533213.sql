-- part of a query repo
-- query name: trades seed generator
-- query link: https://dune.com/queries/4533213


SELECT evt_block_time,evt_tx_hash,evt_index,tokenOut,tokenIn,evt_block_number,amountOut,amountIn
FROM balancer_v3_base.vault_evt_swap