-- part of a query repo
-- query name: etherfi managed position study (ethereum)
-- query link: https://dune.com/queries/6693073


WITH api_call AS (
    SELECT http_get('https://indexer-v3.api.arrakis.finance/v3/indexer/private/1/0x2ce07af7401c94e11ee2d70dd84743950fe913d1/historical/vault-vs-holding?startDate=2025-08-04T03:00:00.000Z&endDate=2026-02-13T11:16:59.106Z') as response
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
