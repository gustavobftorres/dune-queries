-- part of a query repo
-- query name: get rate ethereum wsteth
-- query link: https://dune.com/queries/3530173


select block_number, trace_address, output, bytearray_to_uint256("output")/1e18 as rate,
    CASE 
    WHEN output >= coalesce(lag(output) over (order by block_number, trace_address), 0x00) THEN 'UP' 
    ELSE 'down' end as diff
from ethereum.traces
where to = 0x72D07D7DcA67b8A406aD1Ec34ce969c90bFEE768 -- wstETH rate provider
and input = 0x679aefce -- getRate()
and block_number >= 13013299 -- contract creation block
and success
order by 1,2