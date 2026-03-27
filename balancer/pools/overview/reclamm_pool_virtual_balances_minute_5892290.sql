-- part of a query repo
-- query name: reCLAMM Pool Virtual Balances Minute
-- query link: https://dune.com/queries/5892290


with date_range as (
    -- USDT in the mainnet. The minute column is indexed, faster to merge with liquidity information.
    select "minute" 
    from prices.usd 
    WHERE blockchain = 'ethereum' 
        and minute >= date_trunc('day', TIMESTAMP '{{start}}')
        AND contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
),
pool_tokens AS (
    SELECT 
        pool,
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[1]), '$.token'), 3)) as token_a, 
        from_hex(substr(json_extract_scalar(json_parse(tokenConfig[2]), '$.token'), 3)) as token_b 
    FROM balancer_v3_multichain.vault_evt_poolregistered where pool = {{pool}}
),
initial_virtual_balances AS (
    SELECT 
        VBU.contract_address as pool,
        virtualBalanceA / 1e18 as initial_virtual_balance_a,
        virtualBalanceB / 1e18 as initial_virtual_balance_b
    FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated VBU
    WHERE contract_address = {{pool}}
        AND date_trunc('day', VBU.evt_block_time) <= date_trunc('day', TIMESTAMP '{{start}}')
    ORDER BY VBU.evt_block_time DESC
    LIMIT 1
),
virtual_balances as (
    SELECT 
        date_trunc('minute', VBU.evt_block_time) as "minute",
        VBU.contract_address as pool,
        MAX(virtualBalanceA) / 1e18 as virtual_balance_a,
        MAX(virtualBalanceB) / 1e18 as virtual_balance_b 
    FROM balancer_v3_multichain.reclammpool_evt_virtualbalancesupdated VBU
    WHERE contract_address = {{pool}}
        AND VBU.evt_block_time >= date_trunc('day', TIMESTAMP '{{start}}')
        AND VBU.evt_block_time <= TIMESTAMP '{{end}}'
    GROUP BY date_trunc('minute', VBU.evt_block_time), VBU.contract_address
),
virtual_balances_with_swap AS (
    SELECT
        date_trunc('minute', S.evt_block_time) as "minute",
        S.contract_address as pool,
        virtual_balance_a,
        virtual_balance_b
    FROM balancer_v3_multichain.vault_evt_swap S
    LEFT JOIN virtual_balances VB ON VB."minute" = date_trunc('minute', S.evt_block_time)
    WHERE S.pool = {{pool}} 
        AND S.evt_block_time >= date_trunc('day', TIMESTAMP '{{start}}')
        AND S.evt_block_time <= TIMESTAMP '{{end}}'
),
virtual_balances_with_swap_flagged AS (
    SELECT *,
        SUM(IF(virtual_balance_a IS NOT NULL, 1, 0))
            OVER (ORDER BY "minute" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM virtual_balances_with_swap
),
virtual_balances_with_swap_filled AS (
    SELECT 
        "minute",
        "pool",
        MAX(virtual_balance_a) OVER (PARTITION BY grp ORDER BY "minute") AS virtual_balance_a,
        MAX(virtual_balance_b) OVER (PARTITION BY grp ORDER BY "minute") AS virtual_balance_b
    FROM virtual_balances_with_swap_flagged
),
virtual_balances_with_rebalancing AS (
    SELECT 
        "minute",
        virtual_balance_a,
        virtual_balance_b,
        CASE
            WHEN LEAD(virtual_balance_a) OVER (ORDER BY "minute") > virtual_balance_a AND LEAD(virtual_balance_b) OVER (ORDER BY "minute") < virtual_balance_b THEN 1
            WHEN LEAD(virtual_balance_a) OVER (ORDER BY "minute") < virtual_balance_a AND LEAD(virtual_balance_b) OVER (ORDER BY "minute") > virtual_balance_b THEN 1
            WHEN LAG(virtual_balance_a) OVER (ORDER BY "minute") > virtual_balance_a AND LAG(virtual_balance_b) OVER (ORDER BY "minute") < virtual_balance_b THEN 1
            WHEN LAG(virtual_balance_a) OVER (ORDER BY "minute") < virtual_balance_a AND LAG(virtual_balance_b) OVER (ORDER BY "minute") > virtual_balance_b THEN 1
            ELSE 0
        END as "rebalancing"
    FROM virtual_balances_with_swap_filled
),
virtual_balances_all_minutes as (
    SELECT 
        DR.minute as "minute",
        VB.virtual_balance_a,
        VB.virtual_balance_b,
        VB.rebalancing
    FROM date_range DR
    LEFT JOIN virtual_balances_with_rebalancing VB
        ON VB.minute = DR.minute
),
virtual_balances_flagged AS (
    SELECT *,
        SUM(IF(virtual_balance_a IS NOT NULL, 1, 0))
            OVER (ORDER BY "minute" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM virtual_balances_all_minutes
),
virtual_balances_filled AS (
    SELECT 
        "minute",
        MAX(virtual_balance_a) OVER (PARTITION BY grp ORDER BY "minute") AS virtual_balance_a,
        MAX(virtual_balance_b) OVER (PARTITION BY grp ORDER BY "minute") AS virtual_balance_b,
        MAX(rebalancing) OVER (PARTITION BY grp ORDER BY "minute") AS rebalancing
    FROM virtual_balances_flagged
)
SELECT
    "minute",
    IF(VBF.virtual_balance_a IS NULL, IVB.initial_virtual_balance_a, VBF.virtual_balance_a) as virtual_balance_a,
    IF(VBF.virtual_balance_b IS NULL, IVB.initial_virtual_balance_b, VBF.virtual_balance_b) as virtual_balance_b,
    IF(VBF."rebalancing" IS NULL, 0, VBF."rebalancing") AS "rebalancing"
FROM virtual_balances_filled VBF
LEFT JOIN initial_virtual_balances IVB ON IVB.pool IS NOT NULL
ORDER BY "minute"
