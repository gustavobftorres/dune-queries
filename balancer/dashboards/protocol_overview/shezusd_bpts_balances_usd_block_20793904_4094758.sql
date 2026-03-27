-- part of a query repo
-- query name: shezUSD BPTs Balances, USD @ Block 20793904
-- query link: https://dune.com/queries/4094758


SELECT
    wallet_address,
    wallet_balance,
    gauge_balance,
    aura_balance,
    beefy_balance,
    total_balance / (SELECT SUM(total_balance) FROM query_4093396) AS balance_pct,
    (total_balance / (SELECT SUM(total_balance) FROM query_4093396)) *
    (SELECT SUM(token_balance_usd) FROM query_4094652) AS balance_usd
FROM query_4093396
