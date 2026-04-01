-- part of a query repo
-- query name: veBAL Breakdown
-- query link: https://dune.com/queries/6676121


WITH vebal_data AS (
    SELECT
        wallet_address,
        wallet_label,
        vebal_power,
        vebal_pct
    FROM query_6676090
),

vlaura_data AS (
    SELECT
        wallet_address,
        wallet_label,
        voting_power as vlaura_power,
        voting_power_pct as vlaura_pct
    FROM query_6675399
),

-- Get Aura's total veBAL power
aura_vebal AS (
    SELECT 
        vebal_power as aura_total_vebal
    FROM vebal_data
    WHERE wallet_label = 'Aura'
),

-- Get total veBAL to calculate if Aura exceeds 45%
total_vebal AS (
    SELECT SUM(vebal_power) as total FROM vebal_data
),

-- Calculate Aura's effective veBAL (capped at 45%)
aura_effective_vebal AS (
    SELECT
        CASE 
            WHEN av.aura_total_vebal / tv.total > 0.45 
            THEN tv.total * 0.45  -- Cap at 45%
            ELSE av.aura_total_vebal  -- Use actual if under 45%
        END as effective_aura_vebal
    FROM aura_vebal av
    CROSS JOIN total_vebal tv
),

-- Calculate each vlAURA holder's share of Aura's EFFECTIVE veBAL
vlaura_share_of_aura AS (
    SELECT
        wallet_address,
        wallet_label,
        vlaura_power,
        vlaura_pct,
        vlaura_pct * aev.effective_aura_vebal as vebal_power_from_vlaura
    FROM vlaura_data vl
    CROSS JOIN aura_effective_vebal aev
),

-- Direct veBAL holders (excluding Aura)
direct_vebal AS (
    SELECT
        wallet_label as entity_label,
        0 as vlaura_power,
        vebal_power as direct_vebal_power,
        0 as vebal_power_from_vlaura
    FROM vebal_data
    WHERE wallet_label != 'Aura'
),

-- vlAURA holders with their Aura veBAL share
vlaura_vebal AS (
    SELECT
        wallet_label as entity_label,
        vlaura_power,
        0 as direct_vebal_power,
        vebal_power_from_vlaura
    FROM vlaura_share_of_aura
),

-- Combine both
combined AS (
    SELECT * FROM direct_vebal
    UNION ALL
    SELECT * FROM vlaura_vebal
),

-- Aggregate by entity label
final_aggregation AS (
    SELECT
        entity_label,
        SUM(vlaura_power) as vlaura_power,
        SUM(direct_vebal_power) as direct_vebal_power,
        SUM(vebal_power_from_vlaura) as vebal_power_from_vlaura,
        SUM(direct_vebal_power) + SUM(vebal_power_from_vlaura) as total_voting_power
    FROM combined
    GROUP BY entity_label
),

total_vebal_power AS (
    SELECT SUM(total_voting_power) as total FROM final_aggregation
),

total_vlaura_power AS (
    SELECT SUM(vlaura_power) as total FROM vlaura_data
)

SELECT
    fa.entity_label,
    fa.vlaura_power,
    fa.vlaura_power / tvl.total as vlaura_pct,
    fa.direct_vebal_power,
    fa.vebal_power_from_vlaura,
    fa.total_voting_power,
    fa.total_voting_power / tv.total as total_voting_power_pct,
    fa.direct_vebal_power / tv.total as direct_vebal_power_pct,
    fa.vebal_power_from_vlaura / tv.total as vebal_power_from_vlaura_pct
FROM final_aggregation fa
CROSS JOIN total_vebal_power tv
CROSS JOIN total_vlaura_power tvl
WHERE fa.total_voting_power > 0 OR fa.vlaura_power > 0
ORDER BY fa.total_voting_power DESC
