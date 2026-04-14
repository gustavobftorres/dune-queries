"""
Incrementally sync Dune UI queries into this repository.

Flow:
  1. List team-visible queries via Dune Query Management API.
  2. Detect new IDs (not in queries.yml) and recently updated managed IDs.
  3. Fetch SQL for candidates and update/create local .sql files.
  4. Append newly discovered IDs to queries.yml under category "unclassified".

The script supports a local fixture mode for deterministic tests.

Usage:
    python scripts/sync_from_dune_incremental.py
    python scripts/sync_from_dune_incremental.py --dry-run
    python scripts/sync_from_dune_incremental.py --fixtures-dir scripts/tests/fixtures/sync
"""

from __future__ import annotations

import argparse
import codecs
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal local envs
    def load_dotenv(*_args, **_kwargs):
        return False

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

HEADER_MARKER = "-- part of a query repo"
DEFAULT_LOOKBACK_HOURS = 30
DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
DEFAULT_WORKFLOW_FILE = "sync_from_dune.yml"


def parse_iso8601(value: str | None) -> dt.datetime | None:
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        parsed = dt.datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed
    except ValueError:
        return None


def sanitize_name(name: str, max_len: int = 60) -> str:
    sanitized = re.sub(r"[^a-z0-9]+", "_", name.lower().strip()).strip("_")
    return (sanitized or "query")[:max_len]


def ensure_trailing_newline(text: str) -> str:
    return text if text.endswith("\n") else f"{text}\n"


def build_header(name: str, query_id: int) -> str:
    return (
        f"{HEADER_MARKER}\n"
        f"-- query name: {name}\n"
        f"-- query link: https://dune.com/queries/{query_id}\n\n\n"
    )


def recursive_get_first_str(payload: Any, keys: tuple[str, ...]) -> str | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value
        for value in payload.values():
            found = recursive_get_first_str(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = recursive_get_first_str(value, keys)
            if found:
                return found
    return None


def recursive_collect_strings(payload: Any, keys: tuple[str, ...]) -> set[str]:
    out: set[str] = set()
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in keys:
                if isinstance(value, str) and value.strip():
                    out.add(value.strip())
                elif isinstance(value, (dict, list)):
                    out |= recursive_collect_strings(value, keys)
            if isinstance(value, (dict, list)):
                out |= recursive_collect_strings(value, keys)
    elif isinstance(payload, list):
        for value in payload:
            out |= recursive_collect_strings(value, keys)
    return out


def recursive_get_first_int(payload: Any, keys: tuple[str, ...]) -> int | None:
    if isinstance(payload, dict):
        for key in keys:
            value = payload.get(key)
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        for value in payload.values():
            found = recursive_get_first_int(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = recursive_get_first_int(value, keys)
            if found is not None:
                return found
    return None


def recursive_get_first_datetime(payload: Any, keys: tuple[str, ...]) -> dt.datetime | None:
    if isinstance(payload, dict):
        for key in keys:
            parsed = parse_iso8601(payload.get(key))
            if parsed:
                return parsed
        for value in payload.values():
            found = recursive_get_first_datetime(value, keys)
            if found:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = recursive_get_first_datetime(value, keys)
            if found:
                return found
    return None


def ensure_sql_with_header(sql_text: str, name: str, query_id: int) -> str:
    sql_text = ensure_trailing_newline(sql_text)
    if HEADER_MARKER in sql_text:
        return sql_text
    return build_header(name, query_id) + sql_text


@dataclass
class QueryMetadata:
    query_id: int
    name: str
    created_at: dt.datetime | None
    updated_at: dt.datetime | None
    owner: str | None


@dataclass
class QueryContent:
    query_id: int
    name: str
    sql: str


class DuneHttpClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def _request_json(self, url: str) -> dict[str, Any] | list[Any]:
        req = urllib.request.Request(
            url,
            headers={
                "X-Dune-Api-Key": self.api_key,
                "Accept": "application/json",
                "User-Agent": "balancer-dune-sync/1.0",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {body}") from exc

    @staticmethod
    def _extract_items(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("queries", "data", "results", "items"):
                val = payload.get(key)
                if isinstance(val, list):
                    return [x for x in val if isinstance(x, dict)]
        return []

    @staticmethod
    def _extract_next_offset(payload: dict[str, Any] | list[Any]) -> int | None:
        if not isinstance(payload, dict):
            return None
        direct = payload.get("next_offset")
        if isinstance(direct, int):
            return direct
        if isinstance(direct, str) and direct.isdigit():
            return int(direct)
        pagination = payload.get("pagination")
        if isinstance(pagination, dict):
            nested = pagination.get("nextOffset")
            if isinstance(nested, int):
                return nested
            if isinstance(nested, str) and nested.isdigit():
                return int(nested)
        return None

    def list_queries(self, limit: int = 100) -> list[QueryMetadata]:
        offset = 0
        seen = set()
        out: list[QueryMetadata] = []

        while True:
            query_string = urllib.parse.urlencode({"limit": limit, "offset": offset})
            url = f"{DUNE_API_BASE_URL}/queries?{query_string}"
            payload = self._request_json(url)
            items = self._extract_items(payload)
            if not items:
                break

            for item in items:
                qid = recursive_get_first_int(item, ("id", "query_id", "queryId"))
                if qid is None or qid in seen:
                    continue
                seen.add(qid)
                name = recursive_get_first_str(item, ("name", "query_name", "title")) or f"query_{qid}"
                owner_parts = recursive_collect_strings(
                    item,
                    (
                        "owner",
                        "owner_name",
                        "author",
                        "username",
                        "handle",
                        "display_name",
                        "team",
                        "team_name",
                        "team_slug",
                        "team_handle",
                    ),
                )
                owner = " | ".join(sorted(owner_parts)) if owner_parts else None
                created_at = recursive_get_first_datetime(item, ("created_at", "createdAt"))
                updated_at = recursive_get_first_datetime(item, ("updated_at", "updatedAt", "last_modified_at"))
                out.append(
                    QueryMetadata(
                        query_id=qid,
                        name=name,
                        created_at=created_at,
                        updated_at=updated_at,
                        owner=owner,
                    )
                )

            next_offset = self._extract_next_offset(payload)
            if next_offset is not None and next_offset != offset:
                offset = next_offset
                continue
            if len(items) < limit:
                break
            offset += limit

        return out

    def get_query(self, query_id: int) -> QueryContent:
        payload = self._request_json(f"{DUNE_API_BASE_URL}/query/{query_id}")
        if not isinstance(payload, (dict, list)):
            raise RuntimeError(f"Unexpected query payload type for {query_id}: {type(payload).__name__}")

        name = recursive_get_first_str(payload, ("name", "query_name", "title")) or f"query_{query_id}"
        sql = recursive_get_first_str(payload, ("query_sql", "sql"))
        if not sql:
            raise RuntimeError(f"No SQL field found for query {query_id}")

        return QueryContent(query_id=query_id, name=name, sql=sql)


class FixtureDuneClient:
    def __init__(self, fixtures_dir: Path):
        self.fixtures_dir = fixtures_dir

    def list_queries(self, limit: int = 100) -> list[QueryMetadata]:
        # Simulate pagination by reading list_queries_page{n}.json when present.
        pages: list[Path] = []
        for path in sorted(self.fixtures_dir.glob("list_queries_page*.json")):
            pages.append(path)
        if not pages:
            single = self.fixtures_dir / "list_queries.json"
            if not single.exists():
                raise FileNotFoundError(f"Fixture not found: {single}")
            pages.append(single)

        client = DuneHttpClient(api_key="fixture")
        out: list[QueryMetadata] = []
        seen = set()

        for page in pages:
            payload = json.loads(page.read_text(encoding="utf-8"))
            items = client._extract_items(payload)
            for item in items:
                qid = recursive_get_first_int(item, ("id", "query_id", "queryId"))
                if qid is None or qid in seen:
                    continue
                seen.add(qid)
                name = recursive_get_first_str(item, ("name", "query_name", "title")) or f"query_{qid}"
                owner_parts = recursive_collect_strings(
                    item,
                    (
                        "owner",
                        "owner_name",
                        "author",
                        "username",
                        "handle",
                        "display_name",
                        "team",
                        "team_name",
                        "team_slug",
                        "team_handle",
                    ),
                )
                owner = " | ".join(sorted(owner_parts)) if owner_parts else None
                created_at = recursive_get_first_datetime(item, ("created_at", "createdAt"))
                updated_at = recursive_get_first_datetime(item, ("updated_at", "updatedAt", "last_modified_at"))
                out.append(QueryMetadata(qid, name, created_at, updated_at, owner))
        return out

    def get_query(self, query_id: int) -> QueryContent:
        fixture = self.fixtures_dir / f"query_{query_id}.json"
        if not fixture.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture}")
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        name = recursive_get_first_str(payload, ("name", "query_name", "title")) or f"query_{query_id}"
        sql = recursive_get_first_str(payload, ("query_sql", "sql"))
        if not sql:
            raise RuntimeError(f"Fixture query payload for {query_id} has no SQL field")
        return QueryContent(query_id=query_id, name=name, sql=sql)


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip().lower() for part in value.split(",") if part.strip()]


def parse_bool_env(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def row_matches_owner_allowlist(row: QueryMetadata, allowlist: list[str], require_match: bool) -> bool:
    if not allowlist:
        return True
    owner_text = (row.owner or "").lower()
    if not owner_text:
        return not require_match
    return any(token in owner_text for token in allowlist)


def load_manifest_entries(queries_yml_path: Path) -> tuple[list[Any], dict[int, str | None]]:
    data = yaml.safe_load(queries_yml_path.read_text(encoding="utf-8")) or {}
    entries = data.get("query_ids", []) or []
    id_to_category: dict[int, str | None] = {}
    for entry in entries:
        if isinstance(entry, dict):
            qid = int(entry["id"])
            id_to_category[qid] = entry.get("category")
        else:
            qid = int(entry)
            id_to_category[qid] = None
    return entries, id_to_category


def append_manifest_entries(queries_yml_path: Path, new_ids: list[int], category: str) -> None:
    if not new_ids:
        return
    with queries_yml_path.open("a", encoding="utf-8") as f:
        for qid in new_ids:
            f.write(f"\n  - id: {qid}\n    category: {category}\n")


def find_existing_file(query_id: int, search_root: Path) -> Path | None:
    suffix = f"_{query_id}.sql"
    for dirpath, _, filenames in os.walk(search_root):
        dir_path = Path(dirpath)
        if ".git" in dir_path.parts or "scripts" in dir_path.parts:
            continue
        for filename in filenames:
            if filename.endswith(suffix):
                return dir_path / filename
    return None


def get_previous_success_timestamp(
    repo_slug: str | None,
    github_token: str | None,
    current_run_id: str | None,
    workflow_file: str,
) -> dt.datetime | None:
    if not repo_slug or not github_token:
        return None

    url = (
        f"https://api.github.com/repos/{repo_slug}/actions/workflows/{workflow_file}/runs"
        "?status=success&per_page=20"
    )
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {github_token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "balancer-dune-sync/1.0",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None

    runs = payload.get("workflow_runs", []) if isinstance(payload, dict) else []
    for run in runs:
        run_id = str(run.get("id", ""))
        if current_run_id and run_id == current_run_id:
            continue
        ts = parse_iso8601(run.get("updated_at") or run.get("created_at"))
        if ts:
            return ts
    return None


def write_summary(summary_path: Path, summary: dict[str, Any]) -> None:
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print("SYNC_SUMMARY_JSON_START")
    print(json.dumps(summary, sort_keys=True))
    print("SYNC_SUMMARY_JSON_END")

    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as f:
            f.write(f"sync_summary_path={summary_path}\n")
            f.write(f"sync_changes_total={summary.get('changes_total', 0)}\n")
            f.write(f"sync_new_count={summary.get('new_count', 0)}\n")
            f.write(f"sync_updated_count={summary.get('updated_count', 0)}\n")
            f.write(f"sync_failed_count={summary.get('failed_count', 0)}\n")


def sync_queries(
    repo_root: Path,
    client: DuneHttpClient | FixtureDuneClient,
    lookback_hours: int,
    dry_run: bool,
    default_new_category: str,
    owner_allowlist: list[str],
    require_owner_match: bool,
    workflow_file: str,
    summary_path: Path,
) -> dict[str, Any]:
    queries_yml = repo_root / "queries.yml"
    if not queries_yml.exists():
        raise FileNotFoundError(f"queries.yml not found at {queries_yml}")

    _, manifest_map = load_manifest_entries(queries_yml)
    manifest_ids = set(manifest_map.keys())

    list_rows = client.list_queries(limit=100)
    filtered_rows = [
        row for row in list_rows if row_matches_owner_allowlist(row, owner_allowlist, require_owner_match)
    ]
    filtered_ids = {row.query_id for row in filtered_rows}
    excluded_by_owner_ids = sorted(
        row.query_id for row in list_rows if row.query_id not in filtered_ids
    )
    dune_by_id = {row.query_id: row for row in filtered_rows}
    dune_ids = set(dune_by_id.keys())
    now = dt.datetime.now(dt.timezone.utc)

    previous_success = get_previous_success_timestamp(
        repo_slug=os.environ.get("GITHUB_REPOSITORY"),
        github_token=os.environ.get("GITHUB_TOKEN"),
        current_run_id=os.environ.get("GITHUB_RUN_ID"),
        workflow_file=workflow_file,
    )
    anchor = previous_success or now
    watermark = anchor - dt.timedelta(hours=lookback_hours)

    new_ids = sorted(dune_ids - manifest_ids)
    candidate_updated_ids: list[int] = []
    for qid in sorted(dune_ids & manifest_ids):
        updated_at = dune_by_id[qid].updated_at
        if updated_at and updated_at > watermark:
            candidate_updated_ids.append(qid)

    candidate_ids = sorted(set(new_ids) | set(candidate_updated_ids))

    changed_files: list[str] = []
    new_files: list[str] = []
    updated_files: list[str] = []
    appended_manifest_ids: list[int] = []
    failed_ids: list[int] = []
    skipped_ids: list[int] = []
    unchanged_ids: list[int] = []

    print(
        f"Listed {len(list_rows)} queries from Dune "
        f"(owner-filtered to {len(filtered_rows)}). "
        f"new_ids={len(new_ids)} updated_candidates={len(candidate_updated_ids)} "
        f"watermark={watermark.isoformat()}"
    )

    for qid in candidate_ids:
        is_new = qid in new_ids
        try:
            query = client.get_query(qid)
        except Exception as exc:
            print(f"SKIP {qid}: failed to fetch query ({type(exc).__name__}: {exc})")
            failed_ids.append(qid)
            continue

        if is_new:
            target_dir = repo_root / "balancer" / default_new_category
            file_path = target_dir / f"{sanitize_name(query.name)}_{qid}.sql"
            final_sql = ensure_sql_with_header(query.sql, query.name, qid)

            if not file_path.exists() or file_path.read_text(encoding="utf-8") != final_sql:
                if dry_run:
                    print(f"DRY-RUN CREATE {file_path}")
                else:
                    target_dir.mkdir(parents=True, exist_ok=True)
                    file_path.write_text(final_sql, encoding="utf-8")
                changed_files.append(str(file_path.relative_to(repo_root)))
                new_files.append(str(file_path.relative_to(repo_root)))
            else:
                unchanged_ids.append(qid)

            appended_manifest_ids.append(qid)
            continue

        existing = find_existing_file(qid, repo_root)
        if existing is None:
            print(f"SKIP {qid}: managed query has no local file match")
            skipped_ids.append(qid)
            continue

        final_sql = ensure_sql_with_header(query.sql, query.name, qid)
        current_sql = existing.read_text(encoding="utf-8")
        if current_sql == final_sql:
            unchanged_ids.append(qid)
            continue

        if dry_run:
            print(f"DRY-RUN UPDATE {existing}")
        else:
            existing.write_text(final_sql, encoding="utf-8")

        rel = str(existing.relative_to(repo_root))
        changed_files.append(rel)
        updated_files.append(rel)

    appended_manifest_ids = sorted(set(appended_manifest_ids))
    if appended_manifest_ids:
        if dry_run:
            print(
                "DRY-RUN APPEND queries.yml entries: "
                + ", ".join(str(qid) for qid in appended_manifest_ids)
            )
        else:
            append_manifest_entries(queries_yml, appended_manifest_ids, category=default_new_category)
            changed_files.append("queries.yml")

    summary = {
        "timestamp_utc": now.isoformat(),
        "dry_run": dry_run,
        "workflow_file": workflow_file,
        "listed_queries_count": len(list_rows),
        "owner_filtered_queries_count": len(filtered_rows),
        "owner_allowlist": owner_allowlist,
        "require_owner_match": require_owner_match,
        "excluded_by_owner_ids": excluded_by_owner_ids,
        "watermark_utc": watermark.isoformat(),
        "previous_success_utc": previous_success.isoformat() if previous_success else None,
        "lookback_hours": lookback_hours,
        "new_ids_detected": new_ids,
        "updated_candidate_ids": candidate_updated_ids,
        "new_ids_appended_to_manifest": appended_manifest_ids,
        "changed_files": sorted(set(changed_files)),
        "new_files": sorted(set(new_files)),
        "updated_files": sorted(set(updated_files)),
        "failed_ids": sorted(set(failed_ids)),
        "skipped_ids": sorted(set(skipped_ids)),
        "unchanged_ids": sorted(set(unchanged_ids)),
        "new_count": len(set(new_files)),
        "updated_count": len(set(updated_files)),
        "failed_count": len(set(failed_ids)),
        "changes_total": len(set(changed_files)),
    }

    write_summary(summary_path, summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally sync Dune queries into this repository.")
    parser.add_argument("--dry-run", action="store_true", help="Do not write files.")
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=int(os.environ.get("SYNC_LOOKBACK_HOURS", str(DEFAULT_LOOKBACK_HOURS))),
        help=f"Safety buffer before last successful run (default {DEFAULT_LOOKBACK_HOURS}).",
    )
    parser.add_argument(
        "--new-category",
        default="unclassified",
        help="Category path under balancer/ for newly discovered query IDs.",
    )
    parser.add_argument(
        "--owner-allowlist",
        default=os.environ.get("SYNC_OWNER_ALLOWLIST", ""),
        help="Comma-separated owner/team tokens to allow (case-insensitive contains).",
    )
    parser.add_argument(
        "--require-owner-match",
        action="store_true",
        default=parse_bool_env(os.environ.get("SYNC_REQUIRE_OWNER_MATCH"), default=False),
        help="If set, drop rows with missing/unknown owner metadata when owner allowlist is provided.",
    )
    parser.add_argument(
        "--summary-path",
        default="/tmp/sync_from_dune_summary.json",
        help="Where to write JSON summary.",
    )
    parser.add_argument(
        "--workflow-file",
        default=DEFAULT_WORKFLOW_FILE,
        help=f"Workflow filename used for previous-run watermark lookup (default: {DEFAULT_WORKFLOW_FILE}).",
    )
    parser.add_argument(
        "--fixtures-dir",
        default=None,
        help="Read API payloads from a local fixture directory (no network calls).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")

    fixtures_dir = Path(args.fixtures_dir).resolve() if args.fixtures_dir else None
    summary_path = (repo_root / args.summary_path).resolve()

    if fixtures_dir:
        client = FixtureDuneClient(fixtures_dir=fixtures_dir)
        print(f"Using fixtures from {fixtures_dir}")
    else:
        api_key = os.environ.get("DUNE_API_KEY")
        if not api_key:
            raise SystemExit("DUNE_API_KEY is required")
        if not os.environ.get("GITHUB_TOKEN"):
            print("WARNING: GITHUB_TOKEN missing; previous-success watermark lookup will be skipped.")
        if not os.environ.get("GITHUB_REPOSITORY"):
            print("WARNING: GITHUB_REPOSITORY missing; previous-success watermark lookup will be skipped.")
        client = DuneHttpClient(api_key=api_key)

    summary = sync_queries(
        repo_root=repo_root,
        client=client,
        lookback_hours=args.lookback_hours,
        dry_run=args.dry_run,
        default_new_category=args.new_category.strip("/"),
        owner_allowlist=parse_csv_list(args.owner_allowlist),
        require_owner_match=args.require_owner_match,
        workflow_file=args.workflow_file,
        summary_path=summary_path,
    )

    if summary["failed_count"] > 0:
        print(
            "WARNING: Some queries failed to fetch. "
            f"failed_ids={','.join(str(x) for x in summary['failed_ids'])}"
        )


if __name__ == "__main__":
    main()
