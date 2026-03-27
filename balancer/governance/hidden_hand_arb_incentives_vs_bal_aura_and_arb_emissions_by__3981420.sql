-- part of a query repo
-- query name: Hidden Hand ARB incentives vs. BAL, AURA and ARB emissions by gauge, 2nd phase of STIP
-- query link: https://dune.com/queries/3981420


WITH incentives_and_emissions AS(
    SELECT 
        DATE_TRUNC('month', evt_block_time) AS month,
        gauge,
        SUM(amount_usd) AS emissions_usd,
        sum(amount_usd_sans_aura) AS emissions_usd_sans_aura,
        sum(amount_usd_sans_arb) AS emissions_usd_sans_arb,
        sum(amount_usd_sans_aura_and_arb) AS emissions_usd_sans_aura_and_arb     
    FROM query_3981342
    GROUP BY 1, 2
),

bribs AS(
    SELECT 
        DATE_TRUNC('month', evt_block_time) AS month,
        address,
        name,
        SUM(amount_usd) AS brib_amount_usd
    FROM query_3976357
    GROUP BY 1, 2, 3
)

SELECT
    i.month AS month,
    i.gauge,
    l.name,
    emissions_usd,
    emissions_usd_sans_aura,
    emissions_usd_sans_arb,
    emissions_usd_sans_aura_and_arb,
    brib_amount_usd,
    emissions_usd / brib_amount_usd AS ratio,
    emissions_usd_sans_aura / brib_amount_usd AS ratio_sans_aura,
    emissions_usd_sans_arb / brib_amount_usd AS ratio_sans_arb,
    emissions_usd_sans_aura_and_arb / brib_amount_usd AS ratio_sans_aura_and_arb
FROM incentives_and_emissions i
LEFT JOIN bribs b ON i.month = b.month
AND b.address = i.gauge
LEFT JOIN labels.balancer_gauges l ON i.gauge = l.address
ORDER BY 1 DESC, 4 DESc
--check for match with https://aura.defilytica.com/#/voting-incentives