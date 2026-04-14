# Balancer query discovery

This document records how we catalog existing Balancer-related Dune content and how to populate [`queries.yml`](../queries.yml) before running [`scripts/pull_from_dune.py`](../scripts/pull_from_dune.py).

**Last updated:** 2026-04-10

## Primary discovery path: Query Management API

The default discovery path is now the documented Dune Query Management API:

- `GET /api/v1/queries` to list query metadata (`id`, `created_at`, `updated_at`, owner, etc.)
- `GET /api/v1/query/{id}` to fetch SQL + details for individual queries

This repository uses that API in CI via [`scripts/sync_from_dune_incremental.py`](../scripts/sync_from_dune_incremental.py) and `.github/workflows/sync_from_dune.yml` to keep repo and Dune UI aligned.

## Browser scraping is now fallback-only

If API behavior changes, or if you need a one-off manual inventory from a specific workspace UI page, use the Playwright helper in [`scraping/`](../scraping/).

### Team workspace fallback: automating the full query list

If your team’s queries are listed at:

`https://dune.com/workspace/t/<team_slug>/queries`

(e.g. [balancer team queries](https://dune.com/workspace/t/balancer/queries)), you can avoid opening each query by hand.

**Implemented: [`scraping/`](../scraping/)** (fallback tool; not used in CI)

1. **Playwright** — [`scraping/list_workspace_queries.py`](../scraping/list_workspace_queries.py) opens the workspace URL in Chromium, uses your Dune login (or a saved `storage_state.json` — **gitignored**), scrolls until the list stabilizes, collects `/queries/<id>` links, and writes YAML or plain IDs. See [`scraping/README.md`](../scraping/README.md).
2. **Internal API (optional)** — In Chrome DevTools → Network, reload the workspace page and find the XHR/fetch response that contains query IDs. If it accepts your API key or session cookie, a small `requests` script could call it. Treat this as **fragile**; never commit cookies or HAR files.
3. **Categories** — The workspace UI does not assign repo taxonomy categories. Use `--default-category` for a bulk placeholder, then edit `queries.yml`, or export `--format ids` and assign `category:` by hand.

If the scraper breaks after a Dune UI change, use manual discovery steps below.

## Step 1: Official Balancer dashboards (starting points)

Use these as the primary inventory. Open each dashboard while logged into Dune, then collect query IDs (see Step 2).

| Theme | URL | Suggested `category` in `queries.yml` |
|-------|-----|--------------------------------------|
| Protocol overview | [dune.com/balancer/overview](https://dune.com/balancer/overview) | metric-specific (for example `metrics/volume/protocol`) or cross-metric `analysis/*` |
| Pools | [dune.com/balancer/pools](https://dune.com/balancer/pools) | pool-scoped metrics (for example `metrics/liquidity/pool`, `metrics/tvl/pool`) |
| CoW AMM (official) | [dune.com/balancer/balancer-cowswap-amm](https://dune.com/balancer/balancer-cowswap-amm) | overlaps `cowamm/` (already in manifest) |
| Team profile | [dune.com/balancer](https://dune.com/balancer) / [dune.com/balancerlabs](https://dune.com/balancerlabs) | browse linked dashboards and queries |

Also search Dune for “Balancer”, “veBAL”, “Balancer governance”, “Balancer volume” and add any **team-owned** dashboards you rely on.

## Step 2: Export or copy query IDs

Choose one workflow:

1. **API-first (recommended)**: Use `GET /api/v1/queries` with your team API key and collect IDs from response metadata.
2. **Dune “GitHub” / query repo export** (recommended by [DuneQueryRepo](https://github.com/duneanalytics/DuneQueryRepo)): From the dashboard editor, use the flow that lists query IDs for syncing to a repo (if available in your Dune UI version).
3. **Per-widget**: Click each visualization → “View query” → copy the numeric ID from `https://dune.com/queries/<id>/...`.
4. **Browser devtools**: On the dashboard, use the Network tab while the dashboard loads; filter for API responses that reference `query_id` or `/queries/` (works when you are logged in).

## Step 3: Ownership check

Only queries owned by the **Balancer team** (same team as `DUNE_API_KEY` in GitHub Actions) can be updated by this repository.

- In the Dune UI, confirm the query owner / team.
- Or call the Query API / use `dune-client`: `get_query(id)` and compare `team_id` / `user_id` to your org.

If a query is community-owned but needed in this repo, **fork** it under the Balancer team (new ID), then point dashboards at the new query and list the **new** ID in `queries.yml`.

## Step 4: Add entries to `queries.yml`

For each discovered query:

```yaml
  - id: 1234567
    category: metrics/volume/protocol   # must match a path under balancer/, e.g. analysis/comparisons
```

Use the folder layout under `balancer/` (see [README](../README.md)). CoW AMM queries already tracked under `cowamm/` can stay as bare integers in the manifest.

## Step 5: Spellbook / data catalog (reference)

These are **tables**, not queries. They are the main building blocks for Balancer analytics on Dune (non-exhaustive; use Dune search / MCP `searchTables` for updates):

- **Aggregated:** `balancer.liquidity`, `balancer.pools_metrics_daily`, `balancer.trades`, `balancer.protocol_fees` (when present for your chains)
- **Per-chain v2:** `balancer_v2_<chain>.liquidity`, `balancer_v2_<chain>.pools_fees`, `balancer_v2_<chain>.pools_tokens_weights`, Vault swap / balance events
- **Per-chain v3:** `labels.balancer_v3_pools_<chain>`, `balancer_v3_*` decoded sources (varies by chain)
- **Governance / incentives:** `balancer.gauge_mappings` and related spellbook models

When writing new queries, always filter partitioned tables (e.g. `block_date`, `day`) per [efficient queries](https://docs.dune.com/query-engine/writing-efficient-queries).

## Step 6: Ingestion

After `queries.yml` lists all Balancer-owned query IDs with categories:

```bash
pip install -r scripts/requirements.txt
cp .env.test .env   # add DUNE_API_KEY
python scripts/pull_from_dune.py
python scripts/validate.py
sqlfluff lint balancer/
```

Then open a PR; merge triggers sync for changed `.sql` files.
