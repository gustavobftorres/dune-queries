-- part of a query repo
-- query name: tx 0xdfc8d4d10e390de1551155a54a45bb163508ea9a13b67a793a8fd17df09807f5
-- query link: https://dune.com/queries/4184283


    SELECT 
        call_block_number,
        call_block_date,
        call_block_time,
        call_tx_index,
        call_tx_hash,
        contract_address AS pool_address,
        output_0 AS current_invariant,
        balancesLiveScaled18,
        _0 --round up 1, round down 1
    FROM balancer_testnet_sepolia.StablePool_call_computeInvariant
    WHERE 1 = 1
    AND call_success
    AND call_tx_hash = 0xdfc8d4d10e390de1551155a54a45bb163508ea9a13b67a793a8fd17df09807f5

--current invariant (menor de todas), depois nova e por ultimo calculo com balances ajustados (considerando yield)