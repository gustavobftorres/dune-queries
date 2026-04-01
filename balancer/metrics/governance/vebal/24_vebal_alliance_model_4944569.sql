-- part of a query repo
-- query name: '24 veBAL Alliance Model
-- query link: https://dune.com/queries/4944569


WITH bal_prices AS (
    SELECT
        DATE_TRUNC('month', minute) AS block_month,
        APPROX_PERCENTILE(price, 0.5) AS median_bal_price
    FROM prices.usd
    WHERE contract_address = 0xba100000625a3754423978a60c9317c58a424e3D
    AND DATE_TRUNC('year', minute) = TIMESTAMP '2024-01-01'
    AND blockchain = 'ethereum'
    GROUP BY 1
),

vebal_price AS (
    SELECT
        day AS block_date,
        bpt_price AS vebal_price
    FROM balancer.bpt_prices
    WHERE contract_address = 0x5c6ee304399dbdb9c8ef030ab642b10820db8f56
    AND DATE_TRUNC('year', day) = TIMESTAMP '2024-01-01'
    AND blockchain = 'ethereum'
),

vebal_supply AS (
    SELECT
        day AS block_date,
        SUM(vebal_balance) AS total_vebal
    FROM balancer_ethereum.vebal_balances_day
    WHERE DATE_TRUNC('year', day) = TIMESTAMP '2024-01-01'
    GROUP BY 1
),

vebal_fees AS (
    SELECT
        DATE_TRUNC('month', f.day) AS block_month,
        SUM(
            CASE 
                WHEN c.symbol IS NOT NULL THEN protocol_fee_collected_usd * 0.125
                WHEN c.symbol IS NULL THEN protocol_fee_collected_usd * 0.825
            END
        ) AS fees_to_vebal
    FROM balancer.protocol_fee f
    LEFT JOIN dune.balancer.dataset_core_pools c 
        ON f.blockchain = c.network
        AND f.pool_id = c.pool
    WHERE DATE_TRUNC('year', day) = TIMESTAMP '2024-01-01'
    AND protocol_fee_collected_usd < 1e8
    GROUP BY 1
),

bal_emissions AS (
    SELECT
        DATE_TRUNC('month', time) AS block_month,
        SUM(day_rate) AS month_rate
    FROM query_3140829
    WHERE DATE_TRUNC('year', time) = TIMESTAMP '2024-01-01'
    GROUP BY 1
),

protocol_fees AS (
    SELECT
        '{{partner}}' AS partner,
        DATE_TRUNC('month', f.day) AS block_month,
        SUM(f.treasury_fee_usd) AS redirected_fees,
        SUM(f.protocol_fee_collected_usd) AS protocol_fees,
        (SUM(f.treasury_fee_usd) / p.vebal_price) AS vebal_buyback
    FROM balancer.protocol_fee f
    JOIN vebal_price p 
    ON p.block_date = DATE_TRUNC('month', f.day)
    WHERE DATE_TRUNC('year', f.day) = TIMESTAMP '2024-01-01'
    AND f.pool_id = {{partner_pool}}
    GROUP BY 1, 2, p.vebal_price
),

vebal_balance AS (
    SELECT 
        pf.*,
        vs.total_vebal,
        SUM(vebal_buyback) OVER (ORDER BY pf.block_month) AS vebal_balance
    FROM protocol_fees pf
    JOIN vebal_supply vs
    ON pf.block_month = vs.block_date
    WHERE partner IS NOT NULL
),

vebal_share AS (
    SELECT
        pv.*,
        vebal_balance / (total_vebal + vebal_balance) AS vebal_share
    FROM vebal_balance pv
),

partner_emissions AS (
    SELECT
        vs.*,
        be.month_rate AS monthly_bal_emissions,
        be.month_rate * vebal_share AS bal_emissions_commanded,
        be.month_rate * vebal_share * bp.median_bal_price AS bal_emissions_commanded_usd
    FROM vebal_share vs
    LEFT JOIN bal_emissions be 
    ON vs.block_month = be.block_month
    LEFT JOIN bal_prices bp 
    ON vs.block_month = bp.block_month
)

SELECT 
    partner,
    pe.block_month,
    DATE_FORMAT(pe.block_month, '%Y-%m') AS month_formatted,
    protocol_fees AS protocol_fees_usd,
    redirected_fees AS redirected_fees_usd,
    redirected_fees AS fees_to_be_offset_usd,
    vebal_buyback,
    vebal_balance,
    vebal_share,
    (vf.fees_to_vebal * vebal_share) AS vebal_fees_received_usd,
    bal_emissions_commanded_usd,
    bal_emissions_commanded
FROM partner_emissions pe
JOIN vebal_fees vf
ON vf.block_month = pe.block_month
ORDER BY 2
