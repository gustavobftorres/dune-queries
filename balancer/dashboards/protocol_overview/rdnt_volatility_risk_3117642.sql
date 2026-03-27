-- part of a query repo
-- query name: RDNT Volatility Risk
-- query link: https://dune.com/queries/3117642


WITH 
    RDNTWETH_ratio AS (
         SELECT date_trunc('hour', block_time) as time, avg(ratio) as avg_ratio
         FROM( SELECT block_time, token_bought_amount/token_sold_amount as ratio, amount_usd
               FROM dex.trades WHERE token_sold_symbol = 'RDNT'
               AND block_time >= DATE('2023-03-19') -- pair created
               AND token_pair = 'RDNT-WETH' AND (token_sold_amount != 0 or token_bought_amount != 0) 
               
               union all
               
               SELECT block_time, token_bought_amount/token_sold_amount as ratio, amount_usd
               FROM dex.trades WHERE token_bought_symbol = 'WETH'
               AND block_time >= DATE('2023-03-19') -- pair created 
               AND token_pair = 'RDNT-WETH' AND (token_sold_amount != 0 or token_bought_amount != 0) ) x
        GROUP BY 1 
        ORDER BY 1 DESC 
    ),
    
    get_WETH_price as (SELECT day as time, round(avg(price), 2) as avg_price
        FROM(SELECT date_trunc('hour', minute) as day, price 
        FROM prices.usd WHERE blockchain = 'arbitrum' AND symbol = 'WETH') x
    GROUP BY 1
    ORDER BY 1 DESC 
    ), 
    
    RDNT_price as (
    SELECT time, cast((avg_ratio * avg_price)as double) as RDNT_price 
    FROM RDNTWETH_ratio 
    left join get_WETH_price using (time)
    ORDER BY 1 DESC 
    ),
    
    log_data as (
      SELECT time, log_returns
      FROM (SELECT time, ln(RDNT_price/lag(RDNT_price) over (ORDER BY time)) as log_returns
            FROM RDNT_price
            ORDER BY time DESC
           ) x
      WHERE log_returns is not null AND log_returns > 0 -- ignore negative value
    ),
    
    volatility as (
        SELECT date_trunc('day', time) as time, sqrt(variance(log_returns)) as volatility_risk_RDNT
        FROM log_data
        GROUP BY 1
    ),
    
    summary_table as (
    SELECT *
    FROM volatility
    )

    SELECT *
    FROM summary_table
    ORDER BY time DESC