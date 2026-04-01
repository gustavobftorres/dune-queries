-- part of a query repo
-- query name: GNO/wstETH reCLAMM Price Ranges
-- query link: https://dune.com/queries/5803465


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
FROM "query_5809855(chain='gnosis', pool='0xa50085ff1dfa173378e7d26a76117d68d5eba539')"