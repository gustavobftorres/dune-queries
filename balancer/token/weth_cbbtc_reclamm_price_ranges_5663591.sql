-- part of a query repo
-- query name: WETH/cbBTC reCLAMM Price Ranges
-- query link: https://dune.com/queries/5663591


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
FROM "query_5809855(chain='base', pool='0x19aeb8168d921bb069c6771bbaff7c09116720d0')"
