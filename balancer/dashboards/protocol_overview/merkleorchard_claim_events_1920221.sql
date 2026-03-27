-- part of a query repo
-- query name: MerkleOrchard Claim events
-- query link: https://dune.com/queries/1920221


select
    chain,
    claimer,
    token,
    distributor,
    distributionId as distribution_id,
    amount as claimed_amount
from (
    select 'arbitrum' as chain, *
    from balancer_arbitrum.MerkleOrchard_evt_DistributionClaimed
    where claimer != '0xaf23dc5983230e9eeaf93280e312e57539d098d0'
    union
    select 'polygon' as chain, *
    from balancer_v2_polygon.MerkleOrchard_evt_DistributionClaimed
    where claimer != '0xee071f4b516f69a1603da393cde8e76c40e5be85'
    union
    select 'ethereum' as chain, *
    from balancer_v2_ethereum.MerkleOrchard_evt_DistributionClaimed
    where claimer != '0x10a19e7ee7d7f8a52822f6817de8ea18204f2e4f'
) claims
