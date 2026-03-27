-- part of a query repo
-- query name: AAVE reCLAMM Liquidity Depth Scenarios
-- query link: https://dune.com/queries/6681604


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_ethereum.vault_evt_poolregistered 
    WHERE pool = 0x9d1Fcf346eA1b073de4D5834e25572CC6ad71f4d
),
price_data AS (
    SELECT 
        price,
        ROW_NUMBER() OVER (ORDER BY minute DESC) as rn
    FROM prices.usd 
    WHERE blockchain = 'ethereum'
        AND contract_address = 0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9  -- AAVE
        AND minute >= TIMESTAMP '2026-02-01'
),
latest_price AS (
    SELECT price
    FROM price_data
    WHERE rn = 1
),
latest_balances AS (
    SELECT
        balance_token_in / 1e18 as balance_a,
        balance_token_out / 1e18 as balance_b,
        virtual_balance_token_in / 1e18 as virtual_balance_a,
        virtual_balance_token_out / 1e18 as virtual_balance_b,
        ROW_NUMBER() OVER (ORDER BY block_number DESC) as rn
    FROM query_6681074
),
balancer_state AS (
    SELECT
        balance_a,
        balance_b,
        virtual_balance_a,
        virtual_balance_b,
        (balance_a + virtual_balance_a) * (balance_b + virtual_balance_b) as invariant,
        (balance_b + virtual_balance_b) / (balance_a + virtual_balance_a) as spot_price
    FROM latest_balances
    WHERE rn = 1
)
SELECT
    'Current' as scenario,
    0 as tvl_increase_pct,
    LP.price * (sqrt(BS.invariant / (1.001 * BS.spot_price)) - BS.balance_a - BS.virtual_balance_a) as depth_minus_0_5pct,
    LP.price * (sqrt(BS.invariant / (0.999 * BS.spot_price)) - BS.balance_a - BS.virtual_balance_a) as depth_plus_0_5pct
FROM balancer_state BS
CROSS JOIN latest_price LP

UNION ALL

SELECT
    '+10% TVL',
    10,
    LP.price * (sqrt((BS.invariant * 1.1 * 1.1) / (1.001 * BS.spot_price)) - (BS.balance_a * 1.1) - (BS.virtual_balance_a * 1.1)),
    LP.price * (sqrt((BS.invariant * 1.1 * 1.1) / (0.999 * BS.spot_price)) - (BS.balance_a * 1.1) - (BS.virtual_balance_a * 1.1))
FROM balancer_state BS
CROSS JOIN latest_price LP

UNION ALL

SELECT
    '+25% TVL',
    25,
    LP.price * (sqrt((BS.invariant * 1.25 * 1.25) / (1.001 * BS.spot_price)) - (BS.balance_a * 1.25) - (BS.virtual_balance_a * 1.25)),
    LP.price * (sqrt((BS.invariant * 1.25 * 1.25) / (0.999 * BS.spot_price)) - (BS.balance_a * 1.25) - (BS.virtual_balance_a * 1.25))
FROM balancer_state BS
CROSS JOIN latest_price LP

UNION ALL

SELECT
    '+50% TVL',
    50,
    LP.price * (sqrt((BS.invariant * 1.5 * 1.5) / (1.001 * BS.spot_price)) - (BS.balance_a * 1.5) - (BS.virtual_balance_a * 1.5)),
    LP.price * (sqrt((BS.invariant * 1.5 * 1.5) / (0.999 * BS.spot_price)) - (BS.balance_a * 1.5) - (BS.virtual_balance_a * 1.5))
FROM balancer_state BS
CROSS JOIN latest_price LP

UNION ALL

SELECT
    '+100% TVL',
    100,
    LP.price * (sqrt((BS.invariant * 2.0 * 2.0) / (1.001 * BS.spot_price)) - (BS.balance_a * 2.0) - (BS.virtual_balance_a * 2.0)),
    LP.price * (sqrt((BS.invariant * 2.0 * 2.0) / (0.999 * BS.spot_price)) - (BS.balance_a * 2.0) - (BS.virtual_balance_a * 2.0))
FROM balancer_state BS
CROSS JOIN latest_price LP

ORDER BY tvl_increase_pct