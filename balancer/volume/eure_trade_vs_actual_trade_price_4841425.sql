-- part of a query repo
-- query name: EURe Trade vs. Actual Trade Price
-- query link: https://dune.com/queries/4841425


SELECT DISTINCT
    t.block_time,
    get_href(
        get_chain_explorer_tx_hash(
            'gnosis',
            t.tx_hash
        ),
        CAST(t.tx_hash AS VARCHAR)
    ) AS tx_hash,
    tx_to,
    COALESCE(class, 'others') AS label,
    t.amount_usd,
    p.rate,
    p.actual_price,
    p.diff, --rate - actual_rice
    p.diff * t.amount_usd AS leak
FROM balancer.trades t
JOIN query_4841954 p ON t.block_time = p.block_time --checking calls to the rate provider via traces vs. prices on prices.usd
LEFT JOIN dune.balancer.result_balancer_volume_source_classifier l ON t.tx_to= l.channel
AND l.blockchain = 'gnosis'
WHERE t.blockchain = 'gnosis'
AND project_contract_address = 0xdd439304a77f54b1f7854751ac1169b279591ef7
AND p.diff < - 0.001
AND t.amount_usd > 1000
ORDER BY 9 ASC