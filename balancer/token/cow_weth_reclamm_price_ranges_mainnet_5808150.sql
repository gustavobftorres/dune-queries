-- part of a query repo
-- query name: COW/WETH reCLAMM Price Ranges (Mainnet)
-- query link: https://dune.com/queries/5808150


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
FROM "query_5809855(chain='ethereum', pool='0xd321300ef77067d4a868f117d37706eb81368e98')"
