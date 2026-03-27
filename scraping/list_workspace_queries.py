#!/usr/bin/env python3
"""
Collect Dune query IDs from a team workspace listing (browser automation).

Cloudflare often blocks Playwright's bundled Chromium during login. Workarounds:
  - Export cookies from your normal browser after you log in on dune.com, then use
    --cookies dune_cookies.json (see README).
  - Or use your real Chrome: playwright install chrome && --channel chrome

Usage examples:

  python list_workspace_queries.py --login --save-auth storage_state.json --channel chrome

  python list_workspace_queries.py --cookies dune_cookies.json --headless \\
      -o ../queries.workspace.yml --default-category dashboards/protocol_overview

Requires: pip install -r requirements.txt && playwright install chromium
          (optional) playwright install chrome
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Browser, Playwright, sync_playwright

QUERY_HREF_RE = re.compile(r"/queries/(\d+)(?:/|$|\?|#)")


def workspace_queries_url(team_slug: str) -> str:
    return f"https://dune.com/workspace/t/{team_slug}/queries"


def load_playwright_cookies(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit(f"Cookies file must be a JSON array, got {type(data).__name__}")
    for i, c in enumerate(data):
        if not isinstance(c, dict):
            raise SystemExit(f"Cookie entry {i} must be an object")
        for key in ("name", "value", "domain", "path"):
            if key not in c:
                raise SystemExit(f"Cookie entry {i} missing required key {key!r}")
    return data


def launch_browser(p: Playwright, headed: bool, channel: str | None) -> Browser:
    kwargs: dict = {"headless": not headed}
    if channel:
        kwargs["channel"] = channel
    return p.chromium.launch(**kwargs)


def inject_cookies(context, page, cookies: list[dict]) -> None:
    page.goto("https://dune.com/", wait_until="domcontentloaded", timeout=120_000)
    context.add_cookies(cookies)
    page.goto("https://dune.com/", wait_until="domcontentloaded", timeout=120_000)


def extract_ids_from_page(page) -> set[int]:
    ids: set[int] = set()
    for link in page.locator("a[href]").all():
        href = link.get_attribute("href") or ""
        if not href:
            continue
        normalized_href = urljoin("https://dune.com", href)
        m = QUERY_HREF_RE.search(normalized_href)
        if m:
            ids.add(int(m.group(1)))
    return ids


def scroll_collect_all(page, max_rounds: int = 80, stable_needed: int = 4) -> set[int]:
    all_ids: set[int] = set()
    stable = 0
    for _ in range(max_rounds):
        batch = extract_ids_from_page(page)
        if batch <= all_ids:
            stable += 1
            if stable >= stable_needed:
                break
        else:
            stable = 0
        all_ids |= batch
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.evaluate(
            """() => {
            document.querySelectorAll('[style*="overflow"]').forEach(el => {
              try { el.scrollTop = el.scrollHeight; } catch (e) {}
            });
          }"""
        )
        page.wait_for_timeout(1200)
    return all_ids


def emit_yaml(ids: list[int], default_category: str | None, fp) -> None:
    for qid in ids:
        if default_category:
            fp.write(f"  - id: {qid}\n    category: {default_category}\n")
        else:
            fp.write(f"  - {qid}\n")


def run_login_save_auth(
    save_path: Path,
    headed: bool,
    cookies_path: Path | None,
    channel: str | None,
) -> None:
    save_path.parent.mkdir(parents=True, exist_ok=True)
    cookies = load_playwright_cookies(cookies_path) if cookies_path else None
    with sync_playwright() as p:
        browser = launch_browser(p, headed=True, channel=channel)
        context = browser.new_context()
        page = context.new_page()
        if cookies:
            inject_cookies(context, page, cookies)
            print(
                "Cookies injected. If you are not logged in, log in in the browser, "
                "then press Enter to save session.",
                file=sys.stderr,
            )
        else:
            page.goto("https://dune.com/", wait_until="domcontentloaded")
            print(
                "Log in to Dune in the browser (use real Chrome with --channel chrome "
                "if Cloudflare blocks you). When finished, press Enter to save session to:",
                save_path,
                file=sys.stderr,
            )
        input()
        context.storage_state(path=str(save_path))
        browser.close()
    print(f"Saved auth state to {save_path}", file=sys.stderr)


def run_scrape(
    team_slug: str,
    start_url: str | None,
    auth_path: Path | None,
    cookies_path: Path | None,
    headed: bool,
    channel: str | None,
    output: Path | None,
    fmt: str,
    default_category: str | None,
    dump_html: Path | None,
) -> None:
    url = start_url or workspace_queries_url(team_slug)
    cookies = load_playwright_cookies(cookies_path) if cookies_path else None

    with sync_playwright() as p:
        browser = launch_browser(p, headed=headed, channel=channel)

        if auth_path and auth_path.is_file():
            context = browser.new_context(storage_state=str(auth_path))
        else:
            context = browser.new_context()

        page = context.new_page()

        if cookies and not (auth_path and auth_path.is_file()):
            inject_cookies(context, page, cookies)
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)
        else:
            page.goto(url, wait_until="domcontentloaded", timeout=120_000)

        if not auth_path or not auth_path.is_file():
            if not cookies:
                print(
                    "No --auth / --cookies: log in if prompted. When the query list "
                    "is visible, press Enter to continue scraping.",
                    file=sys.stderr,
                )
                input()
        time.sleep(2)
        ids = scroll_collect_all(page)
        final_url = page.url
        try:
            final_title = page.title()
        except Exception:
            final_title = "<unavailable>"
        page_html = page.content() if dump_html else None
        browser.close()

    sorted_ids = sorted(ids)
    if not sorted_ids:
        print("No query IDs found. Check URL, login, or DOM changes on Dune.", file=sys.stderr)
        print(f"Final page URL: {final_url}", file=sys.stderr)
        print(f"Final page title: {final_title}", file=sys.stderr)
        print(
            "Hint: refresh cookies/auth and try --headed (or --channel chrome for Cloudflare).",
            file=sys.stderr,
        )
        if dump_html:
            dump_html.parent.mkdir(parents=True, exist_ok=True)
            if page_html is not None:
                dump_html.write_text(page_html, encoding="utf-8")
            print(f"Wrote page HTML to {dump_html}", file=sys.stderr)
        sys.exit(2)

    out_fp = open(output, "w", encoding="utf-8") if output else sys.stdout
    try:
        if fmt == "ids":
            for qid in sorted_ids:
                out_fp.write(f"{qid}\n")
        elif fmt == "yaml":
            emit_yaml(sorted_ids, default_category, out_fp)
        else:
            raise ValueError(f"Unknown format {fmt}")
    finally:
        if output:
            out_fp.close()

    print(f"Found {len(sorted_ids)} query IDs.", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--team-slug",
        default="balancer",
        help="Team segment in /workspace/t/<slug>/queries (default: balancer)",
    )
    parser.add_argument(
        "--start-url",
        help="Optional full URL to open instead of /workspace/t/<slug>/queries",
    )
    parser.add_argument(
        "--auth",
        type=Path,
        help="Playwright storage_state.json from a prior --login run",
    )
    parser.add_argument(
        "--cookies",
        type=Path,
        metavar="FILE",
        help="JSON array of Playwright cookies (export from browser after manual login)",
    )
    parser.add_argument(
        "--channel",
        choices=("chrome", "msedge"),
        default=None,
        help="Use installed Chrome/Edge instead of bundled Chromium (helps with Cloudflare)",
    )
    parser.add_argument(
        "--save-auth",
        type=Path,
        metavar="PATH",
        help="With --login: write session to this path after you press Enter",
    )
    parser.add_argument(
        "--login",
        action="store_true",
        help="Open dune.com, wait for you to log in, then save --save-auth and exit",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show browser window (default for scrape without --auth)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Hide browser (use with valid --auth or --cookies)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="Write output here instead of stdout",
    )
    parser.add_argument(
        "--format",
        choices=("yaml", "ids"),
        default="yaml",
        help="yaml: lines for queries.yml; ids: one numeric id per line",
    )
    parser.add_argument(
        "--default-category",
        metavar="PATH",
        help="For yaml format: set category for every row (e.g. dashboards/protocol_overview)",
    )
    parser.add_argument(
        "--dump-html",
        type=Path,
        metavar="PATH",
        help="If no query IDs are found, dump final page HTML to this file for debugging",
    )

    args = parser.parse_args()

    if args.cookies and not args.cookies.is_file():
        parser.error(f"--cookies file not found: {args.cookies}")

    if args.login:
        if not args.save_auth:
            parser.error("--login requires --save-auth PATH")
        run_login_save_auth(
            args.save_auth,
            headed=True,
            cookies_path=args.cookies,
            channel=args.channel,
        )
        return

    use_auth_file = args.auth and args.auth.is_file()
    use_cookies = args.cookies and args.cookies.is_file()
    headed = args.headed or (
        not args.headless and not use_auth_file and not use_cookies
    )

    run_scrape(
        team_slug=args.team_slug,
        start_url=args.start_url,
        auth_path=args.auth,
        cookies_path=args.cookies,
        headed=headed,
        channel=args.channel,
        output=args.output,
        fmt=args.format,
        default_category=args.default_category,
        dump_html=args.dump_html,
    )


if __name__ == "__main__":
    main()
