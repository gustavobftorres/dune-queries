-- part of a query repo
-- query name: GNO/USDC.e reCLAMM Price Ranges
-- query link: https://dune.com/queries/5803417


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
FROM "query_5809855(chain='gnosis', pool='0x70b3b56773ace43fe86ee1d80cbe03176cbe4c09', invert_tokens='1')"