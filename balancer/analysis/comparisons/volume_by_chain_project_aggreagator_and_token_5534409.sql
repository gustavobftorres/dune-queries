-- part of a query repo
-- query name: Volume by chain, project, aggreagator and token
-- query link: https://dune.com/queries/5534409


SELECT
    dt.*,
    COALESCE(dat.project, 'Other') as aggregator_project,
    COALESCE(
        dt.amount_usd,
        CASE
            WHEN dt.token_bought_symbol IN ('USDC', 'USDT', 'USD₮0', 'DAI', 'GHO', 'USDe') THEN dt.token_bought_amount
            WHEN dt.token_sold_symbol IN ('USDC', 'USDT', 'USD₮0', 'DAI', 'GHO', 'USDe') THEN dt.token_sold_amount
            WHEN dt.token_bought_symbol = 'EURC' THEN dt.token_bought_amount * 1.08
            WHEN dt.token_sold_symbol = 'EURC' THEN dt.token_sold_amount * 1.08
            WHEN dt.token_bought_symbol LIKE '%GHO%' THEN dt.token_bought_amount
            WHEN dt.token_sold_symbol LIKE '%GHO%' THEN dt.token_sold_amount
        END
    ) as amount_usd_filled
FROM dex.trades dt
LEFT JOIN dex_aggregator.trades dat
    ON dt.tx_hash = dat.tx_hash
    AND dt.evt_index = dat.evt_index
WHERE dt.block_time > NOW() - INTERVAL '{{days}}' day
AND dt.blockchain IN ('arbitrum', 'base', 'ethereum')
AND (
    UPPER(dt.token_bought_symbol) = UPPER('{{token}}')
    OR UPPER(dt.token_sold_symbol) = UPPER('{{token}}')
    OR UPPER(dt.token_bought_symbol) LIKE UPPER('%a{{token}}%')
    OR UPPER(dt.token_sold_symbol) LIKE UPPER('%a{{token}}%')
)
