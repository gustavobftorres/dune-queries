-- part of a query repo
-- query name: reCLAMM IL (last 30 days) AAVE/WETH - Ethereum
-- query link: https://dune.com/queries/5841062


SELECT *,
    token_b_price/token_a_price as "Price Ratio"
FROM "query_5840683(pool='0x9d1fcf346ea1b073de4d5834e25572cc6ad71f4d', token_a='0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9', token_b='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2')"