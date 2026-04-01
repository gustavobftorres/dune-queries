-- part of a query repo
-- query name: shezETH Pool Token Balances at block 20793804
-- query link: https://dune.com/queries/4094663


 WITH prices AS (
        SELECT
            DATE_TRUNC('hour', minute) AS hour,
            contract_address AS token,
            decimals,
            APPROX_PERCENTILE(price, 0.5) AS price
        FROM prices.usd
        WHERE blockchain = 'ethereum'
        AND contract_address IN (0x63a0964a36c34e81959da5894ad888800e17405b, 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0)
        GROUP BY 1, 2, 3
    ),

    dex_prices_1 AS (
        SELECT
            hour,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM dex.prices
         WHERE blockchain = 'ethereum'
        AND contract_address IN (0x63a0964a36c34e81959da5894ad888800e17405b, 0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0)
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),

    dex_prices_2 AS(
        SELECT
            hour,
            token,
            price,
            lag(price) OVER(PARTITION BY token ORDER BY hour) AS previous_price
        FROM dex_prices_1
    ),

    dex_prices AS (
        SELECT
            hour,
            token,
            price,
            LEAD(hour, 1, NOW()) OVER (PARTITION BY token ORDER BY hour) AS hour_of_next_change
        FROM dex_prices_2
        WHERE (price < previous_price * 1e4 AND price > previous_price / 1e4)
    ),

balances AS(
SELECT DISTINCT
    evt_block_time,
    token_address,
    token_symbol,
    SUM(delta_amount) OVER (PARTITION BY token_address) AS token_balance
FROM balancer.token_balance_changes b
WHERE pool_address = 0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382
AND token_address != 0xDb1f2e1655477d08FB0992f82EEDe0053B8Cd382 
AND evt_block_number < 20793804)

SELECT
    token_address,
    token_symbol,
    token_balance,
    token_balance * COALESCE(p1.price, p2.price, 0)
    AS token_balance_usd
FROM balances b
LEFT JOIN prices p1 ON p1.hour = DATE_TRUNC('hour', b.evt_block_time)
AND p1.token = b.token_address
LEFT JOIN dex_prices p2 ON p2.hour <= DATE_TRUNC('hour', b.evt_block_time)
AND DATE_TRUNC('hour', b.evt_block_time) < p2.hour_of_next_change
AND p2.token = b.token_address
WHERE b.evt_block_time = (SELECT MAX(evt_block_time) FROM balances)