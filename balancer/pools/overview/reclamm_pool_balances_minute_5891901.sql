-- part of a query repo
-- query name: reCLAMM Pool Balances Minute
-- query link: https://dune.com/queries/5891901


with date_range as (
    -- USDT in the mainnet. The minute column is indexed, faster to merge with liquidity information.
    select "minute" 
    from prices.usd 
    WHERE blockchain = 'ethereum' 
        and minute >= date_trunc('day', TIMESTAMP '{{start}}')
        and minute <= TIMESTAMP '{{end}}'
        AND contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
),
pool_balances as (
    SELECT 
        date_trunc('minute', call_block_time) as "minute",
        MAX(CAST(json_extract_scalar(json_parse(request), '$.balancesScaled18[0]') AS DOUBLE)) / 1e18 as balance_a, 
        MAX(CAST(json_extract_scalar(json_parse(request), '$.balancesScaled18[1]') AS DOUBLE)) / 1e18 as balance_b
    FROM balancer_v3_multichain.reclammpool_call_onswap 
    WHERE contract_address = {{pool}} 
        AND call_block_time >= TIMESTAMP '{{start}}'
        AND call_block_time <= TIMESTAMP '{{end}}'
    GROUP BY date_trunc('minute', call_block_time)
),
pool_balances_all_minutes AS (
    SELECT 
        DR.minute,
        PB.balance_a,
        PB.balance_b
    FROM date_range DR 
    LEFT JOIN pool_balances PB ON PB.minute = DR.minute
),
pool_balances_flagged AS (
    SELECT *,
        SUM(CASE WHEN balance_a IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM pool_balances_all_minutes
)
SELECT 
    minute,
    MAX(balance_a) OVER (PARTITION BY grp ORDER BY minute) AS token_a_balance,
    MAX(balance_b) OVER (PARTITION BY grp ORDER BY minute) AS token_b_balance
FROM pool_balances_flagged
ORDER BY "minute"
