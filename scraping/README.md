# Workspace query scraping (one-time bootstrap)

Folder name is **`scraping`** (standard spelling) so it is easy to find and ignore in reviews.

This folder holds a **local-only** helper to list all query IDs visible on your Dune **team workspace queries** page, for example:

[https://dune.com/workspace/t/balancer/queries](https://dune.com/workspace/t/balancer/queries)

Dune does not publish a stable public API for this list, so we use **Playwright** (real Chromium) to scroll the page and collect `/queries/<id>` links.

**Not for CI:** Do not run this in GitHub Actions. Do not commit session files.

**After migration:** Prefer creating queries from the repo workflow (`queries.yml` + PRs); you should rarely need this script again.

## Setup

```bash
cd scraping
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
# optional: real Chrome (often passes Cloudflare better than bundled Chromium)
playwright install chrome
```

## Cloudflare / “verify you are human”

Playwright’s **automation browser** is often challenged; **typing email/password in the script does not fix that** (Cloudflare detects automation, not missing credentials).

Use one of these:

### A) Cookies from your normal browser (recommended)

1. Log in to [dune.com](https://dune.com) in **Chrome or Safari** (your daily browser), complete Cloudflare if shown.
2. Export cookies for `dune.com` as a **JSON array** in [Playwright’s cookie format](https://playwright.dev/python/docs/api/class-browsercontext#browser-context-add-cookies):
   - Each object needs: `name`, `value`, `domain` (e.g. `.dune.com`), `path` (usually `/`).
   - Optional: `httpOnly`, `secure`, `sameSite` (`"Lax"` | `"Strict"` | `"None"`).
3. Save as e.g. `dune_cookies.json` (same folder as the script). **Never commit this file** — add it to `.gitignore` locally if you store it here.
4. Run the scraper **without** opening the login page:

```bash
python list_workspace_queries.py --cookies dune_cookies.json --headless \
  -o queries.workspace.snippet.yml --default-category dashboards/protocol_overview
```

Extensions such as **EditThisCookie** / **Cookie-Editor** can export JSON; you may need to tweak keys to match Playwright’s expected shape.

### B) Real Chrome instead of bundled Chromium

```bash
playwright install chrome
python list_workspace_queries.py --login --save-auth storage_state.json --channel chrome
```

Complete login (and Cloudflare) in the **Chrome** window Playwright opens, then press **Enter** in the terminal.

## 1) Save a logged-in session (once per machine / when cookies expire)

```bash
python list_workspace_queries.py --login --save-auth storage_state.json
# or, if Cloudflare blocks bundled Chromium:
python list_workspace_queries.py --login --save-auth storage_state.json --channel chrome
# or, inject cookies then save session:
python list_workspace_queries.py --login --save-auth storage_state.json --cookies dune_cookies.json
```

When the app is ready, return to the terminal and press **Enter**.  
`storage_state.json` is created next to the script. It is **gitignored**.

## 2) Export query IDs

Default target URL is:

`https://dune.com/workspace/t/<team-slug>/queries` (with `--team-slug balancer` by default)

```bash
python list_workspace_queries.py --auth storage_state.json \
  --output queries.workspace.snippet.yml \
  --default-category dashboards/protocol_overview
```

Or using cookies only (no `storage_state.json`):

```bash
python list_workspace_queries.py --cookies dune_cookies.json --headless \
  -o queries.workspace.snippet.yml --default-category dashboards/protocol_overview
```

If you need to override the page directly (for another team/path), pass a full URL:

```bash
python list_workspace_queries.py --cookies dune_cookies.json --headless \
  --start-url https://dune.com/workspace/t/balancer/queries \
  -o queries.workspace.snippet.yml --default-category dashboards/protocol_overview
```

Open `queries.workspace.snippet.yml`, review, merge the blocks into the root [`queries.yml`](../queries.yml) under `query_ids:` (avoid duplicating IDs already listed for `cowamm/`).

### Options

| Flag | Meaning |
|------|---------|
| `--team-slug balancer` | Workspace URL segment (default `balancer`) |
| `--start-url URL` | Open this full URL instead of `/workspace/t/<team>/queries` |
| `--cookies FILE` | Playwright-format cookie JSON (after manual login in a real browser) |
| `--channel chrome` | Use installed Chrome (or `msedge`) instead of bundled Chromium |
| `--format ids` | One numeric ID per line instead of YAML |
| `--headed` | Force visible browser |
| `--headless` | Hide browser (use with `--auth` or `--cookies`) |
| `--dump-html FILE` | If zero IDs are found, save final HTML for inspection |
| `-o FILE` | Write file instead of stdout |

If you omit `--auth` and `--cookies`, the browser opens on the workspace URL; log in, load the list, then press **Enter** in the terminal to start scrolling.

## 3) Pull SQL into the repo

From the repo root (not `scraping/`):

```bash
pip install -r scripts/requirements.txt
# merge snippet into ../queries.yml first
python scripts/pull_from_dune.py
python scripts/validate.py
```

## Troubleshooting

- **Cloudflare in Playwright:** Use `--cookies` from your normal browser or `--channel chrome` (see above). Do not commit cookie files.
- **Zero IDs:** The script now prints final page URL/title to help diagnose login/challenge redirects. Try `--headed`, confirm the list is visible, and optionally add `--dump-html debug/dune-page.html` to inspect what rendered.
- **Session expired:** Refresh cookies or run `--login --save-auth` again.
- **Wrong team/path:** Pass `--team-slug your_team` for standard workspace queries URL, or use `--start-url` for a specific listing page.

See also [`docs/DISCOVERY.md`](../docs/DISCOVERY.md).
