-- part of a query repo
-- query name: Diff
-- query link: https://dune.com/queries/2623322


-- WITH
--     arb as (
--         SELECT 'arbitrum' AS blockchain, * FROM (
--             VALUES
--                 (19694106.26421238, 'Dex Trades'),
--                 (19694106.264212362, 'Balancer Trades in Prod'),
--                 (22280647.307827197, 'New Balancer Trades w/ BPT Prices')
--         ) AS t (vol, query)
--     ),
    
--     eth as (
--         SELECT 'ethereum' AS blockchain, * FROM (
--             VALUES
--                 (67865782.99563022, 'Dex Trades'),
--                 (67865782.99563026, 'Balancer Trades in Prod'),
--                 (128771984.26512934, 'New Balancer Trades w/ BPT Prices')
--         ) AS t (vol, query)
--     ),
    
--     poly as (
--         SELECT 'polygon' AS blockchain, * FROM (
--             VALUES
--                 (20941185.47326574, 'Dex Trades'),
--                 (20941185.47326573, 'Balancer Trades in Prod'),
--                 (24451318.59282536, 'New Balancer Trades w/ BPT Prices')
--         ) AS t (vol, query)
--     ),
    
--     gnosis as (
--         SELECT 'gnosis' AS blockchain, * FROM (
--             VALUES
--                 (74522.40536791107, 'Dex Trades'),
--                 (74522.4053679111, 'Balancer Trades in Prod'),
--                 (3, 'New Balancer Trades w/ BPT Prices')
--         ) AS t (vol, query)
--     ),
    
--     all as (
--         SELECT * FROM arb
--         UNION ALL
--         SELECT * FROM eth
--         UNION ALL
--         SELECT * FROM poly
--         UNION ALL
--         SELECT * FROM gnosis
--     )

-- SELECT * FROM all

-- FROM 2023-06-08 to 2022-06-08 (y-m-d)
WITH
    arb as (
        SELECT 'arbitrum' AS blockchain, * FROM (
            VALUES
                (1042790184.8006765, 'Dex Trades'),
                (1042790184.8006753, 'Balancer Trades in Prod'),
                (1091625675.774852, 'New Balancer Trades w/ BPT Prices')
        ) AS t (vol, query)
    ),
    
    eth as (
        SELECT 'ethereum' AS blockchain, * FROM (
            VALUES
                (9847909912.753008, 'Dex Trades'),
                (9847909912.753008, 'Balancer Trades in Prod'),
                (20777183491.048683, 'New Balancer Trades w/ BPT Prices')
        ) AS t (vol, query)
    ),
    
    poly as (
        SELECT 'polygon' AS blockchain, * FROM (
            VALUES
                (1023784470.3361943, 'Dex Trades'),
                (1023784470.3361923, 'Balancer Trades in Prod'),
                (1321783793.3090196, 'New Balancer Trades w/ BPT Prices')
        ) AS t (vol, query)
    ),
    
    -- gnosis as (
    --     SELECT 'gnosis' AS blockchain, * FROM (
    --         VALUES
    --             (74522.40536791107, 'Dex Trades'),
    --             (74522.4053679111, 'Balancer Trades in Prod'),
    --             (3, 'New Balancer Trades w/ BPT Prices')
    --     ) AS t (vol, query)
    -- ),
    
    all as (
        SELECT * FROM arb
        UNION ALL
        SELECT * FROM eth
        UNION ALL
        SELECT * FROM poly
        -- UNION ALL
        -- SELECT * FROM gnosis
    )

SELECT * FROM all