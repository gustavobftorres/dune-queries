-- part of a query repo
-- query name: LST Pools Liquidity
-- query link: https://dune.com/queries/3382339


WITH pools AS(
    SELECT * 
    FROM dune.balancer.result_lst_pools
),

tokens AS(
SELECT
    DATE_TRUNC('day', minute) AS day,
    q.address,
    q.name,
    q.blockchain,
    t.decimals,
    AVG(price) AS price
FROM query_3382363 q
LEFT JOIN tokens.erc20 t ON q.blockchain = t.blockchain AND q.equivalent = t.contract_address
LEFT JOIN prices.usd p ON q.blockchain = p.blockchain AND q.equivalent = p.contract_address
GROUP BY 1, 2, 3, 4, 5
),


liquidity AS(
    SELECT 
        l.day,
        l.pool_id,
        l.blockchain,
        l.token_address,
        token_balance_raw / POWER(10, t.decimals) * t.price AS aprotocol_liquidity_usd
    FROM balancer.liquidity l
    INNER JOIN pools p ON p.pool_address = l.pool_address AND p.blockchain = l.blockchain
    LEFT JOIN tokens t ON t.address = l.token_address AND t.blockchain = l.blockchain AND t.day = l.day
    WHERE token_symbol IS NULL
),

tvl AS(
SELECT 
    l.day,
    l.pool_id,
    l.pool_address,
    l.pool_symbol,
    l.blockchain,
    l.token_address,
    CASE WHEN 
        token_symbol IS NULL
        THEN aprotocol_liquidity_usd
        ELSE protocol_liquidity_usd
    END AS protocol_liquidity_usd
FROM balancer.liquidity l
INNER JOIN pools p ON p.pool_address = l.pool_address AND p.blockchain = l.blockchain
LEFT JOIN liquidity q ON q.pool_id = l.pool_id AND q.day = l.day AND q.blockchain = l.blockchain AND q.token_address = l.token_address
)

SELECT 
    t.day,
    pool_id,
    pool_symbol,
    pool_address,
    blockchain,
    token_address,
    sum(protocol_liquidity_usd) AS protocol_liquidity_usd,
    sum(protocol_liquidity_usd /  p.median_price_eth) AS protocol_liquidity_eth
FROM tvl t
LEFT JOIN dune.balancer.result_eth_price p ON p.day = t.day
GROUP BY 1, 2, 3, 4, 5, 6