# Dependency Resolution (Phase 4)

Phase 4 objective: map `query_{id}` references, ensure dependencies are present, and consolidate shared intermediate queries under `balancer/views/`.

## Final Status

- `validate.py`: **All checks passed** (no unresolved `query_{id}` dependencies).
- Shared Query Views tracked in `queries.yml` (`category: views`): **42**.
- SQL files in `balancer/views/`: **42**.

## Shared Query Views Imported for Dependency Closure

| Query ID | File | Query name |
|---|---|---|
| `1541516` | `balancer/views/v3_vouch_registry_1541516.sql` | V3: Vouch Registry |
| `1747157` | `balancer/views/get_chain_explorer_1747157.sql` | get_chain_explorer |
| `2406726` | `balancer/views/linear_pools_2406726.sql` | Linear Pools |
| `2407001` | `balancer/views/linear_pool_params_2407001.sql` | Linear Pool Params |
| `2417365` | `balancer/views/linear_pool_components_2417365.sql` | Linear Pool Components |
| `2452021` | `balancer/views/balancer_v2_lbps_2452021.sql` | Balancer V2 LBPs |
| `2477497` | `balancer/views/balancer_source_query_2477497_2477497.sql` | Balancer_Source_query_2477497 |
| `2511140` | `balancer/views/bal_supply_projection_2511140.sql` | BAL Supply Projection |
| `2972525` | `balancer/views/fjord_all_lbps_test_v4_2972525.sql` | fjord all lbps test v4 |
| `2977657` | `balancer/views/fjord_all_lbps_multi_chain_2977657.sql` | FJORD / ALL LBPs / Multi-chain |
| `3091308` | `balancer/views/polygon_zkevm_gauge_mapping_need_chain_support_to_finish_3091308.sql` | polygon zkevm gauge mapping (need chain support to finish) |
| `3094283` | `balancer/views/gnosis_gauge_mapping_3094283.sql` | gnosis gauge mapping |
| `3094743` | `balancer/views/base_gauge_mapping_3094743.sql` | base gauge mapping |
| `3150087` | `balancer/views/balancer_sankey_3150087.sql` | Balancer Sankey |
| `3333356` | `balancer/views/v3_block_number_interval_from_time_interval_update_3333356.sql` | V3: Block Number Interval from Time Interval (Update) |
| `3779014` | `balancer/views/cross_chain_vebal_boost_addresses_3779014.sql` | Cross-Chain veBAL boost - Addresses |
| `3781645` | `balancer/views/beets_bridged_from_and_to_optimism_txs_3781645.sql` | BEETS bridged from and to OPTIMISM - TXs |
| `3859543` | `balancer/views/humpywallets_3859543.sql` | humpyWallets |
| `4021257` | `balancer/views/cow_protocol_balance_changes_4021257.sql` | CoW Protocol Balance Changes |
| `4021306` | `balancer/views/cow_protocol_classified_balance_changes_4021306.sql` | CoW Protocol Classified Balance Changes |
| `4021555` | `balancer/views/cow_protocol_cows_per_token_4021555.sql` | CoW Protocol CoWs per Token |
| `4021644` | `balancer/views/cow_protocol_token_imbalances_4021644.sql` | CoW Protocol Token Imbalances |
| `4025739` | `balancer/views/cow_protocol_cows_per_batch_4025739.sql` | CoW Protocol CoWs per Batch |
| `4031637` | `balancer/views/prices_from_cow_protocol_trades_4031637.sql` | Prices from CoW Protocol Trades |
| `4056263` | `balancer/views/valid_full_bonding_pools_4056263.sql` | Valid Full Bonding Pools |
| `4428137` | `balancer/views/erc4626_token_prices_4428137.sql` | erc4626_token_prices |
| `4527783` | `balancer/views/uni_v2_amm_lp_and_tvl_4527783.sql` | uni_v2_amm_lp_and_tvl |
| `4717948` | `balancer/views/balancer_cow_swaps_vs_buffer_balances_4717948.sql` | balancer <> CoW swaps vs. buffer balances |
| `4844142` | `balancer/views/cow_amm_vs_uni_pool_vs_token_rebalancing_fork_v2_4844142.sql` | CoW AMM vs Uni Pool vs Token Rebalancing Fork v2 |
| `5044493` | `balancer/views/fluid_blockchains_5044493.sql` | fluid_blockchains |
| `5057796` | `balancer/views/all_balancer_reclamms_5057796.sql` | all balancer reclamms |
| `5143758` | `balancer/views/solver_accounting_invalidate_vouches_5143758.sql` | Solver Accounting: Invalidate vouches |
| `5143848` | `balancer/views/solver_accounting_vouches_5143848.sql` | Solver Accounting: Vouches |
| `5169345` | `balancer/views/manual_pricing_5169345.sql` | manual_pricing |
| `5651828` | `balancer/views/aero_v2_amm_lp_and_tvl_5651828.sql` | aero_v2_amm_lp_and_tvl |
| `5825281` | `balancer/views/tokens_native_5825281.sql` | tokens_native |
| `5875552` | `balancer/views/reclamm_plasma_pool_balances_hourly_5875552.sql` | reCLAMM Plasma Pool Balances Hourly |
| `6521186` | `balancer/views/polygon_query_after_compile_6521186.sql` | Polygon query after compile |
| `6521379` | `balancer/views/optimism_query_after_compile_6521379.sql` | Optimism query after compile |
| `6681074` | `balancer/views/aave_uniswap_v3_vs_simulated_reclamm_6681074.sql` | AAVE Uniswap v3 vs Simulated reCLAMM |
| `6753954` | `balancer/views/vebal_vlaura_materialized_view_6753954.sql` | Vebal vlAura - Materialized View |
| `6754023` | `balancer/views/balancer_swaps_by_token_6754023.sql` | Balancer Swaps by Token |

## External Table Dependencies (Approximate, by schema)

These are table-level dependencies (`FROM/JOIN schema.table`) and are separate from `query_{id}` references.

### Spellbook/Core Data Schemas (Top)

| Schema | Reference count |
|---|---:|
| `dex` | 425 |
| `prices` | 272 |
| `labels` | 229 |
| `tokens` | 161 |
| `balancer_v3` | 13 |
| `dex_aggregator` | 13 |
| `balancer_v2` | 9 |
| `safe` | 2 |

### Other / Community Schemas (Top)

| Schema | Reference count |
|---|---:|
| `balancer` | 693 |
| `balancer_v2_ethereum` | 246 |
| `dune` | 223 |
| `balancer_v2_arbitrum` | 218 |
| `balancer_ethereum` | 214 |
| `balancer_v2_polygon` | 178 |
| `balancer_v2_gnosis` | 107 |
| `balancer_v2_optimism` | 106 |
| `erc20` | 84 |
| `balancer_v2_base` | 78 |
| `balancer_v3_ethereum` | 67 |
| `balancer_v2_avalanche_c` | 62 |
| `balancer_v2_zkevm` | 59 |
| `ethereum` | 59 |
| `balancer_v3_multichain` | 53 |

## Notes

- The schema table above is regex-based and intended as a practical dependency inventory, not a full SQL parser output.
- Query-view dependency closure is now complete for the imported repository set; further unresolved dependencies should only appear when new queries are added.
