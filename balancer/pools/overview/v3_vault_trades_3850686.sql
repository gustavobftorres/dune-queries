-- part of a query repo
-- query name: V3_vault_trades
-- query link: https://dune.com/queries/3850686


       SELECT
            swap.contract_address AS vault,
            swap.evt_block_number,
            swap.evt_block_time AS block_time,
            swap.evt_tx_hash AS tx_hash,
            swap.evt_index,
            swap.pool AS project_contract_address,
            swap.tokenOut AS token_bought_address,
            t1.symbol AS token_bought_symbol,
            swap.tokenIn AS token_sold_address,
            t2.symbol AS token_sold_symbol,
            swap.amountOut AS token_bought_amount_raw,
            swap.amountIn AS token_sold_amount_raw,
            swap.amountOut / POWER(10, COALESCE(t1.decimals, 18)) AS token_bought_amount,
            swap.amountIn / POWER(10, COALESCE(t2.decimals, 18)) AS token_sold_amount,            
            swap.SwapFeePercentage AS swap_fee_percentage,
            swap.tokenIn AS swap_fee_token,
            swap.swapFeeAmount
        FROM
            balancer_testnet_sepolia.Vault_evt_Swap swap
        LEFT JOIN tokens.erc20 t1 ON t1.contract_address = swap.tokenOut AND t1.blockchain = 'sepolia'
        LEFT JOIN tokens.erc20 t2 ON t2.contract_address = swap.tokenIn AND t2.blockchain = 'sepolia'
        ORDER BY 3 DESC