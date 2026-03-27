-- part of a query repo
-- query name: Parameterized reCLAMM Price Ranges
-- query link: https://dune.com/queries/5809855


WITH pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where chain = '{{chain}}' AND pool = {{pool}}
),
pool_tokens_inverted AS (
    SELECT
        pool,
        CASE WHEN {{invert_tokens}} = 0 THEN token_a ELSE token_b END as token_a,
        CASE WHEN {{invert_tokens}} = 0 THEN token_b ELSE token_a END as token_b
    FROM pool_tokens
),
virtual_balance_events AS (
   SELECT 
       evt_block_date as day,
       CASE WHEN {{invert_tokens}} = 0 THEN virtualBalanceA ELSE virtualBalanceB END / 1e18 as virtual_balance_a,
       CASE WHEN {{invert_tokens}} = 0 THEN virtualBalanceB ELSE virtualBalanceA END / 1e18 as virtual_balance_b,
       evt_block_time,
       ROW_NUMBER() OVER (PARTITION BY evt_block_date ORDER BY evt_block_time DESC) as rn
   FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated
   WHERE chain = '{{chain}}'
       AND contract_address = {{pool}}
),
daily_virtual AS (
   SELECT day, virtual_balance_a, virtual_balance_b
   FROM virtual_balance_events
   WHERE rn = 1
),
margin_events AS (
    SELECT 
       evt_block_date as day,
       centerednessMargin / 1e18 as centeredness_margin,
       evt_block_time,
       ROW_NUMBER() OVER (PARTITION BY evt_block_date ORDER BY evt_block_time DESC) as rn
   FROM balancer_v3_multichain.reclammpool_evt_centerednessmarginupdated
   WHERE chain = '{{chain}}'
       AND contract_address = {{pool}}
),
daily_margin AS (
   SELECT day, centeredness_margin
   FROM margin_events
   WHERE rn = 1
),
daily_data AS (
   SELECT 
       l.day,
       MAX(CASE WHEN l.token_address = pt.token_a THEN l.token_balance END) as real_balance_a,
       MAX(CASE WHEN l.token_address = pt.token_b THEN l.token_balance END) as real_balance_b,
       v.virtual_balance_a,
       v.virtual_balance_b,
       m.centeredness_margin
   FROM balancer.liquidity l
   JOIN pool_tokens_inverted pt on pt.pool = l.pool_address
   LEFT JOIN daily_virtual v ON l.day = v.day
   LEFT JOIN daily_margin m ON l.day = m.day
   WHERE l.blockchain = '{{chain}}'
       AND l.pool_address = {{pool}}
   GROUP BY l.day, v.virtual_balance_a, v.virtual_balance_b, m.centeredness_margin
),
filled_data AS (
   SELECT 
       day,
       real_balance_a,
       real_balance_b,
       COALESCE(
           virtual_balance_a,
           LAG(virtual_balance_a) IGNORE NULLS OVER (ORDER BY day)
       ) as virtual_balance_a,
       COALESCE(
           virtual_balance_b,
           LAG(virtual_balance_b) IGNORE NULLS OVER (ORDER BY day)
       ) as virtual_balance_b,
       COALESCE(
           centeredness_margin,
           LAG(centeredness_margin) IGNORE NULLS OVER (ORDER BY day)
       ) as centeredness_margin
   FROM daily_data
),
price_calculations AS (
    SELECT 
        day,
        real_balance_a,
        real_balance_b,
        virtual_balance_a,
        virtual_balance_b,
        (real_balance_b + virtual_balance_b) / (real_balance_a + virtual_balance_a) as spot_price,
        (virtual_balance_b * virtual_balance_b) / ((real_balance_a + virtual_balance_a) * (real_balance_b + virtual_balance_b)) as min_price,
        ((real_balance_a + virtual_balance_a) * (real_balance_b + virtual_balance_b)) / (virtual_balance_a * virtual_balance_a) as max_price,
        (real_balance_a + virtual_balance_a) * (real_balance_b + virtual_balance_b) as invariant,
        centeredness_margin as margin
    FROM filled_data
    WHERE virtual_balance_a IS NOT NULL
)
SELECT 
    day,
    real_balance_a,
    real_balance_b,
    virtual_balance_a,
    virtual_balance_b,
    spot_price,
    min_price,
    max_price,
    invariant / POWER(
        virtual_balance_a + (
            -(virtual_balance_a * (1 + margin)) + 
            SQRT(POWER(virtual_balance_a * (1 + margin), 2) - 4 * margin * (virtual_balance_a * virtual_balance_a - invariant * virtual_balance_a / virtual_balance_b))
        ) / 2, 
    2) as lower_margin,
    invariant / POWER(
        virtual_balance_a + (
            -(virtual_balance_a * (1 + margin) / margin) + 
            SQRT(POWER(virtual_balance_a * (1 + margin) / margin, 2) - 4 * (virtual_balance_a * virtual_balance_a - invariant * virtual_balance_a / virtual_balance_b) / margin)
        ) / 2,
    2) as upper_margin
FROM price_calculations
ORDER BY day DESC
