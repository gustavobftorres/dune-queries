-- part of a query repo
-- query name: Insert arb bots on Polygon
-- query link: https://dune.com/queries/142007


DROP TABLE IF EXISTS dune_user_generated.balancer_arb_bots;

CREATE TABLE dune_user_generated.balancer_arb_bots (
    address bytea,
    author text,
    name text,
    type text
);

WITH arbs AS (
        SELECT
            DISTINCT(t1.tx_to) AS address,
            'arbitrage bot' AS label,
            'dapp usage' AS type,
            'balancerlabs' AS author
        FROM dex.trades t1
        INNER JOIN dex.trades t2
        ON t1.tx_hash = t2.tx_hash
        AND t1.token_a_address = t2.token_b_address
        AND t1.token_b_address = t2.token_a_address
        AND ((t1.project = 'Balancer' AND t2.project = 'Sushiswap') or (t1.project = 'Sushiswap' AND t2.project = 'Balancer'))
        WHERE t1.block_time >= now() - interval '7d'
        AND t2.block_time >= now() - interval '7d'
    )

INSERT INTO dune_user_generated.balancer_arb_bots
SELECT DISTINCT address, author, label, type FROM arbs