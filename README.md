# Balancer Dune Queries

Repository for managing Balancer protocol Dune Analytics queries. Changes merged to `main` are automatically synced to Dune.

## Repository Structure

```
balancer/                 Balancer protocol queries
  metrics/                Primary metric queries (canonical home for new work)
    volume/
      protocol/             Protocol-level volume
      pool/                 Pool-level volume
      token/                Token-pair / token-level volume
      source/               Aggregator/source attribution
      trader/               User / cohort / trader segments
    liquidity/
      protocol/             Protocol-level liquidity
      pool/                 Pool-level liquidity
      token/                Token liquidity views
      utilization/          Liquidity utilization-focused queries
    fees/
      swap/                 Swap fee metrics
      protocol/             Protocol fee metrics
      lp/                   LP fee metrics
    tvl/
      protocol/             Protocol TVL
      pool/                 Pool TVL
      integration/          Integration/project TVL
    revenue/
      protocol/             Protocol revenue
      lp/                   LP revenue
    token/
      price/                Token pricing
      supply/               Supply / emissions / minting
      holders/              Holder distribution and cohorts
      flows/                Inflow/outflow and transfer flows
    governance/
      vebal/                veBAL metrics
      gauges/               Gauge metrics
      voting/               Voting and delegation analytics
      incentives/           Incentive programs and bribes
  analysis/               Cross-metric analytics (new canonical home)
    comparisons/            A vs B benchmarks
    relationships/          Metric-vs-metric, ratio, correlation
    distributions/          Breakdowns, shares, top-N distributions
  support/                Helper/composition queries (non-primary metrics)
    composed/               Query-on-query composition helpers
    selectors/              Selected/calendars/labels/channels helpers
    qa/                     Debug, tests, expected-vs-current checks
    legacy/                 Temporary bucket during migration
  views/                  Shared intermediate queries (Query Views)
  # Legacy directories from initial scrape (kept during migration):
  volume/ liquidity/ fees/ tvl/ revenue/ token/ governance/
  pools/overview pools/weighted pools/stable pools/boosted pools/lbp
  dashboards/protocol_overview
cowamm/                   CoW AMM queries (Balancer CoW AMM product)
uploads/                  CSV files uploaded as Dune tables
scripts/                  Tooling for query management
```

All query file names follow the pattern `descriptive_name_{queryId}.sql`. The query ID is the numeric ID from the Dune URL (`dune.com/queries/{id}`).

### Placement Rules (New Queries)

- Put primary metric queries under `balancer/metrics/<metric>/<scope>/...`.
- Put co-primary/cross-metric outputs (`*_vs_*`, `*_and_*`, ratio, correlation, benchmark) under `balancer/analysis/...`.
- Put helper/composition/selector/debug queries under `balancer/support/...`.
- Keep `balancer/views/` for reusable Query Views consumed by downstream queries.
- Legacy directories remain valid while we migrate older files incrementally.

## Migrating existing Balancer queries from Dune

Phase 2 discovery is documented in [docs/DISCOVERY.md](docs/DISCOVERY.md). To export **all** query IDs from the team workspace list in one go, use the local Playwright helper in [`scraping/`](scraping/README.md) (first-time bootstrap only). Then merge into `queries.yml` and run `pull_from_dune.py` (Phase 3 ingestion).

## Quick Start

### Prerequisites

- Python 3.9+
- A Dune API key from a **Plus plan** or higher (create one at [Dune team settings](https://dune.com/settings/teams))

### Local Setup

```bash
cp .env.test .env             # copy template and fill in your DUNE_API_KEY
pip install -r scripts/requirements.txt
```

### Scripts

| Script | Description | Command |
|--------|-------------|---------|
| `pull_from_dune.py` | Fetch queries from Dune into the repo based on `queries.yml` | `python scripts/pull_from_dune.py` |
| `push_to_dune.py` | Push all managed queries from repo to Dune (manual full sync) | `python scripts/push_to_dune.py` |
| `preview_query.py` | Run a query and display the first 20 rows (uses API credits) | `python scripts/preview_query.py <query_id>` |
| `upload_to_dune.py` | Upload CSV files from `uploads/` to Dune as tables | `python scripts/upload_to_dune.py` |
| `validate.py` | Check naming conventions, manifest consistency, dependencies | `python scripts/validate.py` |

## Adding a New Query

1. Create the query on [dune.com](https://dune.com/queries) and save it. Note the query ID from the URL.
2. Add the ID to `queries.yml` with the target category:
   ```yaml
   - id: 1234567
     category: metrics/volume/protocol
   ```
3. Pull it into the repo:
   ```bash
   python scripts/pull_from_dune.py
   ```
   Or create the file manually: `balancer/metrics/volume/protocol/descriptive_name_1234567.sql`
4. Ensure the file starts with `-- part of a query repo` (the pull script adds this automatically).
5. If the query uses new Jinja parameters (e.g., `{{pool_type}}`), add them to the relevant `.sqlfluff` context file.
6. Test locally:
   ```bash
   sqlfluff lint balancer/metrics/volume/protocol/descriptive_name_1234567.sql
   python scripts/preview_query.py 1234567
   ```
7. Open a PR. SQLFluff runs automatically. Follow the PR template to document your changes.
8. On merge to `main`, CI automatically syncs the query to Dune.

## Updating Queries

Edit the SQL file, open a PR, and merge. CI handles the rest.

## Removing Queries

Deleting a file from the repo does **not** archive the query on Dune. If you want to archive it, do so manually on dune.com. Also remove the ID from `queries.yml`.

## Uploading CSV Tables

Place CSV files in the `uploads/` directory. On merge to `main`, they are uploaded to Dune as tables named `dune.{team_name}.dataset_{filename}` (without the `.csv` extension).

## Query Composition (DRY)

Use Dune [Query Views](https://docs.dune.com/query-engine/query-a-query#query-views) to avoid duplicating logic. Place shared intermediate queries in `balancer/views/` and reference them via `query_{id}` in downstream queries.

For tips on writing efficient queries, see the [Dune guide](https://docs.dune.com/query-engine/writing-efficient-queries).

## CI/CD

- **On PR**: [SQLFluff](https://sqlfluff.com/) lints changed SQL files. The [nitpicker](https://github.com/ethanis/nitpicker) bot flags common issues (e.g., using deprecated `prices.usd`, missing partition filters).
- **On merge to `main`**: Changed `.sql` files are synced to Dune via the [`bh2smith/dune-update`](https://github.com/bh2smith/dune-update) GitHub Action. Changed CSVs in `uploads/` are uploaded via `upload_to_dune.py`.

### Important Notes

- **Ownership**: Queries must be owned by the team whose API key is configured. You cannot update queries owned by other teams.
- **Rollback**: If a bad merge pushes broken SQL, use [Dune's query version history](https://dune.com/docs/app/query-editor/version-history) to revert.
- **File names are not synced**: Renaming a file in the repo does not rename the query on Dune. The `_{queryId}.sql` suffix is what matters -- do not remove it.

## Linting

[SQLFluff](https://sqlfluff.com/) runs on every PR. Install locally:

```bash
pip install sqlfluff
sqlfluff lint balancer/          # lint all Balancer queries
sqlfluff lint cowamm/            # lint CoW AMM queries
sqlfluff fix <file>              # auto-fix a specific file
```

## Contributing

See the [PR template](.github/PULL_REQUEST_TEMPLATE.md) for the required format. Issues can be filed using the [issue templates](.github/ISSUE_TEMPLATE/).

| Issue Type | Use For |
|------------|---------|
| Bug | Data quality issues, broken queries, miscalculations |
| Chart Improvement | Visualization suggestions |
| Query Improvement | SQL enhancements, new columns, performance |
| Question | General questions or suggestions |
