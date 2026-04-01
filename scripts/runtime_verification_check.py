"""
Runtime verification for a Tier-1 query set.

Objective:
  - Verify all shared dependency views are accessible on Dune.
  - Verify representative queries for each non-view category are accessible.
  - Check whether a latest execution result exists for each verified query.

This script does NOT execute every query by default. It validates fetchability
and latest-result availability to provide practical confidence without
triggering expensive full reruns.

Usage:
    python scripts/runtime_verification_check.py
"""

from __future__ import annotations

import argparse
import logging
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
DOTENV_PATH = REPO_ROOT / ".env"
QUERIES_YML = REPO_ROOT / "queries.yml"
REPORT_PATH = REPO_ROOT / "docs" / "RUNTIME_VERIFICATION_REPORT.md"


@dataclass
class CheckResult:
    query_id: int
    category: str
    selected_as: str  # "views" or "representative"
    query_name: str
    fetch_ok: bool
    latest_result_ok: bool
    latest_rows: int | None
    note: str


def load_manifest_entries(path: Path) -> list[dict]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    entries = data.get("query_ids", [])
    normalized: list[dict] = []
    for e in entries:
        if isinstance(e, dict):
            normalized.append({"id": int(e["id"]), "category": e.get("category", "")})
        else:
            normalized.append({"id": int(e), "category": ""})
    return normalized


def select_tier1_set(entries: list[dict]) -> list[tuple[int, str, str]]:
    selected: list[tuple[int, str, str]] = []
    seen_ids: set[int] = set()

    # 1) All shared dependency views.
    for e in entries:
        if e["category"] == "views":
            qid = e["id"]
            if qid in seen_ids:
                continue
            seen_ids.add(qid)
            selected.append((qid, e["category"], "views"))

    # 2) Representative query per non-view category.
    by_category: dict[str, list[int]] = defaultdict(list)
    for e in entries:
        cat = e["category"] or "legacy-no-category"
        if cat == "views":
            continue
        by_category[cat].append(e["id"])

    for cat in sorted(by_category):
        representative = min(by_category[cat])  # deterministic choice
        if representative in seen_ids:
            continue
        seen_ids.add(representative)
        selected.append((representative, cat, "representative"))

    return selected


def fetch_query_name_with_fallback(dune: DuneClient, query_id: int, api_key: str) -> tuple[bool, str, str]:
    # Primary path via dune-client.
    try:
        query = dune.get_query(query_id)
        return True, query.base.name, "ok"
    except Exception as sdk_error:  # pragma: no cover - operational fallback
        # Fallback to raw API to handle occasional dune-client parsing edge cases.
        try:
            resp = requests.get(
                f"https://api.dune.com/api/v1/query/{query_id}",
                headers={"X-Dune-API-Key": api_key},
                timeout=30,
            )
            if resp.status_code != 200:
                return False, "", f"fetch_failed_http_{resp.status_code}"
            payload = resp.json()
            if not isinstance(payload, dict):
                return False, "", "fetch_failed_invalid_payload"
            name = payload.get("name", "")
            if not name:
                return False, "", "fetch_failed_missing_name"
            return True, name, f"ok_fallback_after_{type(sdk_error).__name__}"
        except Exception as fallback_error:  # pragma: no cover
            return False, "", f"fetch_failed_{type(fallback_error).__name__}"


def check_latest_result(dune: DuneClient, query_id: int) -> tuple[bool, int | None, str]:
    try:
        latest = dune.get_latest_result(QueryBase(query_id=query_id))
        rows = len(getattr(latest.result, "rows", []) or [])
        return True, rows, "latest_result_ok"
    except Exception as exc:
        return False, None, f"latest_result_unavailable_{type(exc).__name__}"


def build_report(results: list[CheckResult], check_latest: bool) -> str:
    total = len(results)
    fetch_ok = sum(1 for r in results if r.fetch_ok)
    latest_ok = sum(1 for r in results if r.latest_result_ok)
    by_type = defaultdict(list)
    for r in results:
        by_type[r.selected_as].append(r)

    lines: list[str] = [
        "# Runtime Verification Report",
        "",
        "Tier-1 objective:",
        "- Verify shared dependency views are accessible on Dune.",
        "- Verify one representative query per non-view category is accessible.",
        "- Check latest-result availability for each checked query."
        if check_latest
        else "- Skip latest-result probing (fetchability-only mode).",
        "",
        "## Summary",
        "",
        f"- Queries checked: **{total}**",
        f"- Metadata fetch success: **{fetch_ok}/{total}**",
        f"- Latest-result available: **{latest_ok}/{total}**",
        "",
    ]

    for section in ("views", "representative"):
        rows = by_type.get(section, [])
        if not rows:
            continue
        lines.extend(
            [
                f"## {section.capitalize()} Checks",
                "",
                "| Query ID | Category | Query name | Fetch | Latest result | Note |",
                "|---|---|---|---|---|---|",
            ]
        )
        for r in sorted(rows, key=lambda x: (x.category, x.query_id)):
            fetch = "OK" if r.fetch_ok else "FAIL"
            latest = "OK" if r.latest_result_ok else "N/A"
            if r.latest_rows is not None:
                latest = f"OK ({r.latest_rows} rows)"
            query_name = r.query_name.replace("|", "\\|")
            note = r.note.replace("|", "\\|")
            lines.append(
                f"| `{r.query_id}` | `{r.category}` | {query_name} | {fetch} | {latest} | {note} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Interpretation",
            "",
            "- `Fetch = OK` confirms the query metadata is retrievable with the configured API key.",
            "- `Latest result = OK` means Dune has a recent materialized execution for that query ID.",
            "- `Latest result = N/A` does not necessarily mean the query is broken; often it has never been run recently, result expired, or is private to another context.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check-latest",
        action="store_true",
        help="Also probe latest result availability for each Tier-1 query.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=REPORT_PATH,
        help="Markdown report output path (default: docs/RUNTIME_VERIFICATION_REPORT.md).",
    )
    args = parser.parse_args()

    # Reduce noisy SDK logs for missing latest results.
    logging.getLogger("dune_client.models").setLevel(logging.CRITICAL)
    logging.getLogger("dune_client.api.base").setLevel(logging.WARNING)

    load_dotenv(DOTENV_PATH)
    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        raise SystemExit("DUNE_API_KEY is missing. Set it in .env first.")

    entries = load_manifest_entries(QUERIES_YML)
    targets = select_tier1_set(entries)
    dune = DuneClient.from_env()

    results: list[CheckResult] = []
    for query_id, category, selected_as in targets:
        fetch_ok, query_name, fetch_note = fetch_query_name_with_fallback(dune, query_id, api_key)
        if not fetch_ok:
            results.append(
                CheckResult(
                    query_id=query_id,
                    category=category,
                    selected_as=selected_as,
                    query_name="",
                    fetch_ok=False,
                    latest_result_ok=False,
                    latest_rows=None,
                    note=fetch_note,
                )
            )
            continue

        if args.check_latest:
            latest_ok, latest_rows, latest_note = check_latest_result(dune, query_id)
        else:
            latest_ok, latest_rows, latest_note = False, None, "latest_check_skipped"
        results.append(
            CheckResult(
                query_id=query_id,
                category=category,
                selected_as=selected_as,
                query_name=query_name,
                fetch_ok=True,
                latest_result_ok=latest_ok,
                latest_rows=latest_rows,
                note=latest_note if latest_ok else f"{fetch_note}; {latest_note}",
            )
        )

    report = build_report(results, check_latest=args.check_latest)
    args.output.write_text(report + "\n", encoding="utf-8")
    print(f"Wrote report to {args.output}")
    print(f"Checked {len(results)} Tier-1 queries.")


if __name__ == "__main__":
    main()
