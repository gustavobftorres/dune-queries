-- part of a query repo
-- query name: Buffers Overview
-- query link: https://dune.com/queries/4150879


SELECT
    wrappedToken,
    q.symbol,
    q.blockchain,
    wrapped_balance,
    wrapped_balance * APPROX_PERCENTILE(e.median_price, 0.5) AS wrapped_balance_usd,
    underlying_balance,
    underlying_balance * APPROX_PERCENTILE(p.price, 0.5) AS underlying_balance_usd,
    CONCAT('<a href="https://dune.com/balancer/buffers?blockchain_eb6b35=', q.blockchain, '&erc4626_token_eb6b35=', CAST(q.symbol AS VARCHAR),'&wrapped_token_te05e1=', CAST(wrappedToken AS VARCHAR), '">View Stats ↗</a>')
FROM query_4453217 q
JOIN balancer_v3.erc4626_token_mapping m ON q.wrappedToken = m.erc4626_token
AND q.blockchain = m.blockchain
JOIN prices.usd p ON m.underlying_token = p.contract_address
AND m.blockchain = p.blockchain
AND DATE_TRUNC('day', p.minute) = CURRENT_DATE
JOIN balancer_v3.erc4626_token_prices e ON q.wrappedToken = e.wrapped_token
AND q.blockchain = e.blockchain
WHERE q.rn = 1
AND wrapped_balance > 1
GROUP BY 1, 2, 3, 4, 6
ORDER BY 7 DESC