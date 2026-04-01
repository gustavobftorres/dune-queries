-- part of a query repo
-- query name: 0x675 getRate
-- query link: https://dune.com/queries/3094085


select block_time, output,  * from ethereum.traces
where "to" = 0x67560A970FFaB46D65cB520dD3C2fF4E684f29c2