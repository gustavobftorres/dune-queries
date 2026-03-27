-- part of a query repo
-- query name: vlAURA Delegates Power
-- query link: https://dune.com/queries/6675357


WITH delegator_data AS (
    SELECT
        delegator,
        delegator_label,
        eth_locked,
        base_locked,
        total_locked,
        delegated_to,
        delegated_to_label,
        has_delegated
    FROM query_6675227
),

total_voting_power AS (
    SELECT SUM(eth_locked) as total FROM delegator_data
),

-- Ethereum delegation
eth_delegate_aggregates AS (
    SELECT
        delegated_to as delegate,
        delegated_to_label as delegate_label,
        SUM(eth_locked) as eth_voting_power,
        SUM(CASE WHEN has_delegated = true THEN eth_locked ELSE 0 END) as eth_delegated_power,
        SUM(CASE WHEN has_delegated = false THEN eth_locked ELSE 0 END) as eth_own_power
    FROM delegator_data
    GROUP BY delegated_to, delegated_to_label
    HAVING SUM(CASE WHEN has_delegated = true THEN eth_locked ELSE 0 END) > 0
),

-- Base always stays with owner
base_own_power AS (
    SELECT
        delegator as delegate,
        SUM(base_locked) as base_own_power
    FROM delegator_data
    WHERE base_locked > 0
    GROUP BY delegator
),

top_delegators AS (
    SELECT
        delegated_to,
        delegator_label,
        eth_locked,
        ROW_NUMBER() OVER (PARTITION BY delegated_to ORDER BY eth_locked DESC) as rn
    FROM delegator_data
    WHERE has_delegated = true
)

SELECT
    da.delegate AS delegate_address,
    da.delegate_label,
    COALESCE(da.eth_voting_power, 0) + COALESCE(bp.base_own_power, 0) as voting_power,
    (COALESCE(da.eth_voting_power, 0) / tvp.total) as voting_power_pct,
    COALESCE(da.eth_delegated_power, 0) as delegated_power,
    COALESCE(da.eth_own_power, 0) + COALESCE(bp.base_own_power, 0) as own_power,
    ARRAY_AGG(td.delegator_label ORDER BY td.rn) FILTER (WHERE td.rn <= 3) as top_delegators
FROM eth_delegate_aggregates da
CROSS JOIN total_voting_power tvp
LEFT JOIN base_own_power bp ON bp.delegate = da.delegate
LEFT JOIN top_delegators td ON td.delegated_to = da.delegate AND td.rn <= 3
GROUP BY da.delegate, da.delegate_label, da.eth_voting_power, da.eth_delegated_power, da.eth_own_power, bp.base_own_power, tvp.total
ORDER BY voting_power DESC
