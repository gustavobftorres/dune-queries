-- part of a query repo
-- query name: get_order_id
-- query link: https://dune.com/queries/3974017


SELECt * FROM gnosis_protocol_v2_ethereum.GPv2Settlement_evt_Trade
WHERE evt_tx_hash = 0x96b2ef22f7962211fdc961488a5c857b5779eb110d273921e1b82cc9aa7d5a18