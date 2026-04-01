-- part of a query repo
-- query name: PRL/WETH reCLAMM Price Ranges
-- query link: https://dune.com/queries/5808288


SELECT
    day,
    real_balance_a,
    real_balance_b,
    virtual_balance_a,
    virtual_balance_b,
    spot_price,
    min_price,
    max_price,
    lower_margin,
    upper_margin
FROM "query_5809855(chain='base', pool='0x53d31feb99eccd1375a9ec433d1d7873dfb68263')"