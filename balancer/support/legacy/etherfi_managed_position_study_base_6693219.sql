-- part of a query repo
-- query name: etherfi managed position study (base)
-- query link: https://dune.com/queries/6693219


WITH api_call AS (
    SELECT http_get('https://indexer-v3.api.arrakis.finance/v3/indexer/private/8453/0xc9A96Aba6842370C30A7c1B4AFACBc616FA8bc9e/historical/vault-vs-holding?startDate=2025-08-04T03:00:00.000Z&endDate=2026-02-13T12:56:53.103Z') as response
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
