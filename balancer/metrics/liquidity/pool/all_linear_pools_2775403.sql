-- part of a query repo
-- query name: All Linear Pools
-- query link: https://dune.com/queries/2775403


WITH
    -- query_2407001 = https://dune.com/queries/2407001?sidebar=none Linear Pool Parameters
    -- query_2406726 = https://dune.com/queries/2406726?sidebar=none Linear Pools
    -- query_2417365 = https://dune.com/queries/2417365?sidebar=none Linear Pool Components
    -- Possible Spells Above
    linear_pool_params AS (SELECT * FROM query_2407001),
    poolID_info AS (SELECT distinct blockchain, lending_standard, factory_address, pool, poolID FROM query_2406726),
    main_and_wrapped_tokens AS (SELECT * FROM query_2417365),
    -- Additional Info Below
    pool_liquidity AS (
        SELECT CAST(pool_id as VARCHAR) as pool_id, pool_symbol, token_address, token_balance, pool_liquidity_usd as usd_amount FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY pool_id, token_address ORDER BY day DESC) AS latest_update 
            FROM balancer_v2_ethereum.liquidity
            )
        WHERE latest_update = 1 
    ),
    labels AS (select blockchain, address, name AS pool_symbol from labels.balancer_v2_pools)
    
SELECT 
    CASE WHEN pp.blockchain = 'gnosis' THEN concat('<a href="', 'https://app.balancer.fi/#/', 'gnosis-chain', '/pool/', CAST(id.poolID AS VARCHAR), '" target = "_blank">', '[POOL]', '</a>') 
    ELSE concat('<a href="', 'https://app.balancer.fi/#/', pp.blockchain, '/pool/', CAST(id.poolID AS VARCHAR), '" target = "_blank">', '[POOL]', '</a>') END AS frontend_link,
    pp.blockchain,	
    --pp.lending_standard,
    id.poolID,
    pp.linear_pool,
    COALESCE(l.pool_symbol, pl_1.pool_symbol,
        concat(
            substring(CAST(id.poolID AS VARCHAR),1,5), '...', substring(CAST(id.poolID AS VARCHAR),62)
        )
    ) AS pool_symbol,
    pp.linear_swap_fee,	
    pp.token AS main_token,
    t1.symbol AS main_token_symbol,
    pl_1.usd_amount AS main_token_usd_amount,
    pl_1.token_balance AS main_token_balance,
    pp.lowerTarget AS lower_target,
    pp.upperTarget AS upper_target,
    --mw.main_token, 
    mw.wrapped_token,
    t2.symbol AS wrapped_token_symbol,
    pl_2.usd_amount AS wrapped_token_usd_amount,
    pl_2.token_balance AS wrapped_token_balance,
    id.factory_address,
    pp.composable_stable_pool_id_and_swap_fees
    
FROM linear_pool_params pp 
LEFT JOIN poolID_info id ON pp.linear_pool = id.pool AND pp.blockchain = id.blockchain
LEFT JOIN main_and_wrapped_tokens mw ON pp.linear_pool = mw.pool_token AND pp.blockchain = mw.blockchain
LEFT JOIN (SELECT contract_address, symbol, blockchain FROM tokens.erc20) t1 ON pp.token = t1.contract_address AND pp.blockchain = t1.blockchain
LEFT JOIN (SELECT contract_address, symbol, blockchain FROM tokens.erc20) t2 ON mw.wrapped_token = t2.contract_address AND pp.blockchain = t2.blockchain
--ORDER BY linear_swap_fee

LEFT JOIN pool_liquidity pl_1 ON (CAST(id.poolID as VARCHAR) = pl_1.pool_id AND pp.token = pl_1.token_address)
LEFT JOIN pool_liquidity pl_2 ON (CAST(id.poolID as VARCHAR) = pl_2.pool_id AND mw.wrapped_token = pl_2.token_address)
LEFT JOIN labels l ON (pp.linear_pool = l.address AND pp.blockchain = l.blockchain)
ORDER BY pl_1.usd_amount DESC
    

