-- part of a query repo
-- query name: reCLAMM Plasma BPT Supply Hourly
-- query link: https://dune.com/queries/5874871


WITH all_minutes AS (
    -- Used only to fetch minutes
    SELECT minute
    FROM prices.usd 
    WHERE 
        blockchain = 'ethereum'
        and contract_address = 0xdac17f958d2ee523a2206206994597c13d831ec7
        and minute > TIMESTAMP '{{start}}'
        and minute < now()
),
bpt_supply_raw as (
    SELECT 
        date_trunc('minute', evt_block_time) as "minute", MAX(totalSupply) as total_supply
    FROM balancer_v3_multichain.vault_evt_liquidityadded 
    WHERE pool = {{pool}}
    GROUP BY date_trunc('minute', evt_block_time)

    UNION

    SELECT 
        date_trunc('minute', evt_block_time) as "minute", MAX(totalSupply) as total_supply
    FROM balancer_v3_multichain.vault_evt_liquidityremoved 
    WHERE pool = {{pool}}
    GROUP BY date_trunc('minute', evt_block_time)
),
initial_bpt_supply as (
    SELECT CAST(total_supply as DOUBLE)/1e18 as total_supply FROM bpt_supply_raw WHERE "minute" < TIMESTAMP '{{start}}' ORDER BY "minute" DESC LIMIT 1
),
bpt_supply_all_minutes AS (
    SELECT 
        A.minute,
        BSR.total_supply
    FROM all_minutes A 
    LEFT JOIN bpt_supply_raw BSR ON BSR.minute = A.minute
),
bpt_supply_flagged AS (
    SELECT *,
        SUM(CASE WHEN total_supply IS NOT NULL THEN 1 ELSE 0 END) 
            OVER (ORDER BY minute ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS grp
    FROM bpt_supply_all_minutes
),
bpt_supply_minute as (
    SELECT 
        minute,
        CAST(MAX(total_supply) OVER (PARTITION BY grp ORDER BY minute) AS DOUBLE) / 1e18 AS total_supply
    FROM bpt_supply_flagged
)
SELECT
    date_trunc('hour', "minute") as "hour",
    IF(BSM.total_supply IS NULL, IBS.total_supply, BSM.total_supply) as total_supply
FROM bpt_supply_minute BSM
LEFT JOIN initial_bpt_supply IBS ON IBS.total_supply IS NOT NULL
WHERE minute("minute") = 0