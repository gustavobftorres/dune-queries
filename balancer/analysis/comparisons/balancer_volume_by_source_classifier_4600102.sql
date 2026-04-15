-- part of a query repo
-- query name: Balancer Volume by Source Classifier
-- query link: https://dune.com/queries/4600102


WITH 
    raw_swaps AS (
        SELECT 
            DATE_TRUNC('month', block_date) AS week,
            blockchain,
            tx_to AS channel,
            COUNT(*) AS txns,
            SUM(amount_usd) AS volume
        FROM balancer.trades
        WHERE amount_usd IS NOT NULL
        GROUP BY 1, 2, 3
    ),

    manual_labels AS (
        SELECT address, 'MEV Bot' AS name
        FROM (
            VALUES
                (0xbee3211ab312a8d065c4fef0247448e17a8da000),
                (0x827fbd229f68ed3359a05e772d30481b59a0ad24),
                (0x2d83ff1cb1c79c68fe530d35f439a92a645faded),
                (0x6fc3dc715f1dc99f4f59bf5c3e53dca31e16f29d),
                (0x00000000009e50a7ddb7a7b0e2ee6604fd120e49),
                (0x3057fe9ce0785055d546e974befa47b5e9f0cccc),
                (0x0000000000001ff3684f28c67538d4d072c22734),
                (0xba10edc7542be68bdc543f12de4f6f5fad96a71c),
                (0x36331e299247e5d0d3261e1d9852f6e0cffee95c),
                (0x00c21ca82d94dade0d5d1ed420a4728f58427d21),
                (0x6e2743c18690d05dd2ea1ecb7fe86322fcd6e491),
                (0xd23755eab25cfd840f93be1461da8b3fad75d94e),
                (0xf3e399d5a4b6777941ecc607b733269c27d19a8e),
                (0x75804bc26c185b2df19bd100d91290176b31dd48),
                (0xa3d6f183d0d9dcee644a61c4194360a8d6d66448),
                (0xaf682de1f2e6f710731121a05a44cb3c1b511f7d),
                (0xa2ccdfc0ab477aa3f4e2b7e6c7d7019443e15c98),
                (0x191c1dd35db11e7ddcfdd4c28fe5bc6c55800377),
                (0x00000000008d5f1200332af8a6813cb8377b5bfd),
                (0xbd32122bad41a09f2405bb374a83877d8245079c),
                (0x1a15f443dffcc4a2549b4ffdc1e033135b263dde),
                (0x17c7f76b475d126a43f16371d2993ccbe17f7bdb),
                (0x356688f595ef0f6d2bc03714107c415e289f85a8),
                (0xcc605061b13eb8f16eb8bd220730973a29408c73),
                (0xfeecbe8036d95acf84a2c9b8974fa0ced1c4cc17),
                (0x900bb76ea5ddd93f25456982689d1ec11fc026a8),
                (0xb6f54caed61c318027c022c47b94baf139a99dab),
                (0x81463b0f960f247f704377661ec81c1fd65b5128),
                (0x45675168bd487e4c4a4f70b264b4a395b4df2776),
                (0x0000000000007a8d56014359bf3e98f18b7773f9),
                (0x25665edb09daea7d3c571b5ea2c3bbf086e9cc1d)
        ) AS t(address)
    ),

    all_labels AS (
        SELECT 
            address,
            blockchain,
            name,
            1 AS priority
        FROM query_3004790

        UNION ALL

        SELECT 
            address,
            NULL AS blockchain,
            name,
            2 AS priority
        FROM manual_labels
    ),

    distinct_labels AS (
        SELECT address, blockchain, name
        FROM (
            SELECT
                address,
                blockchain,
                name,
                ROW_NUMBER() OVER (
                    PARTITION BY address, COALESCE(blockchain, 'all')
                    ORDER BY priority DESC
                ) AS rn
            FROM all_labels
        ) x
        WHERE rn = 1
    ),

    channels_with_totals AS (
        SELECT 
            blockchain, 
            channel, 
            SUM(volume) AS volume,
            COUNT(*) AS txns,
            SUM(SUM(volume)) OVER (PARTITION BY blockchain) AS total_volume
        FROM raw_swaps
        GROUP BY 1, 2
    )

SELECT 
    c.blockchain,
    c.channel,
    COALESCE(
        CASE 
            WHEN l.name = 'Arbitrage Bot' THEN 'MEV Bot'
            WHEN l.name IS NOT NULL THEN l.name
            WHEN c.txns >= 100 THEN 'Heavy Trader'
            ELSE 'Others'
        END,
        'Others'
    ) AS class
FROM channels_with_totals c
LEFT JOIN distinct_labels l
    ON c.channel = l.address
   AND (c.blockchain = l.blockchain OR l.blockchain IS NULL)
;
