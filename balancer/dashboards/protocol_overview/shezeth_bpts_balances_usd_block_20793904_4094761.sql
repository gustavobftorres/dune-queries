-- part of a query repo
-- query name: shezETH BPTs Balances, USD @ Block 20793904
-- query link: https://dune.com/queries/4094761


SELECT
    wallet_address,
    wallet_balance,
    gauge_balance,
    aura_balance,
    beefy_balance,
    total_balance / (SELECT SUM(total_balance) FROM query_4093231) AS balance_pct,
    (total_balance / (SELECT SUM(total_balance) FROM query_4093231)) *
    (SELECT SUM(token_balance_usd) FROM query_4094663) AS balance_usd
FROM query_4093231
