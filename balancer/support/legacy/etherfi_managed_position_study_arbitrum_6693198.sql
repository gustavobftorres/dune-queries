-- part of a query repo
-- query name: etherfi managed position study (arbitrum)
-- query link: https://dune.com/queries/6693198


WITH api_call AS (
    SELECT http_get('https://indexer-v3.api.arrakis.finance/v3/indexer/private/42161/0x5654943cc196234efefb1d31c6354b70e365c90f/historical/vault-vs-holding?startDate=2025-08-13T03%3A00%3A00.000Z&endDate=2026-02-13T12%3A56%3A53.103Z
') as response
)

SELECT 
    json_extract_scalar(value, '$.timestamp') as block_date,
    CAST(json_extract_scalar(value, '$.vaultValueUSD') as double) as vault_value_usd,
    CAST(json_extract_scalar(value, '$.holdingValueUSD') as double) as holding_value_usd,
    CAST(json_extract_scalar(value, '$.relativePerformancePct') as double) as relative_performance_pct,
    CAST(json_extract_scalar(value, '$.holdingNormalizedPct') as double) as holding_normalized_pct,
    CAST(json_extract_scalar(value, '$.vaultNormalizedPct') as double) as vault_normalized_pct
FROM 
    api_call,
    UNNEST(
        CAST(json_extract(response, '$.data') AS array(json))
    ) AS t(value)
