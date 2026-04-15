"""
Incrementally sync Dune UI queries into this repository.

Flow:
  1. List team-visible queries via Dune Query Management API.
  2. Detect new IDs (not in queries.yml), recently updated managed IDs, and
     existing unclassified IDs eligible for legacy migration checks.
  3. Fetch SQL for candidates and update/create local .sql files.
  4. Classify new queries into repository taxonomy with deterministic rules and
     optional capped LLM fallback.
  5. Apply legacy policy for stale new/unclassified queries.

Usage:
    python scripts/sync_from_dune_incremental.py
    python scripts/sync_from_dune_incremental.py --dry-run
    python scripts/sync_from_dune_incremental.py --fixtures-dir scripts/tests/fixtures/sync
"""

from __future__ import annotations

import argparse
import codecs
import copy
import datetime as dt
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
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
DEFAULT_WORKFLOW_FILE = "sync_from_dune.yml"
DUNE_API_BASE_URL = "https://api.dune.com/api/v1"
DEFAULT_CLASSIFICATION_RULES = "scripts/classification_rules.yml"
DEFAULT_DASHBOARD_MAP = "scripts/dashboard_category_map.yml"
DEFAULT_LEGACY_DAYS = 180


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


def parse_bool_env(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


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


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


@dataclass
class QueryMetadata:
    query_id: int
    name: str
    created_at: dt.datetime | None
    updated_at: dt.datetime | None
    owner: str | None
    dashboard_id: int | None
    dashboard_slug: str | None


@dataclass
class QueryContent:
    query_id: int
    name: str
    sql: str


@dataclass
class UsageSnapshot:
    credits_used: float
    billing_start: str | None
    billing_end: str | None
    service_credits: dict[str, float]


@dataclass
class ClassificationResult:
    category: str
    method: str
    confidence: float
    top_category: str
    top_score: float
    second_category: str | None
    second_score: float
    votes: list[str]
    used_llm: bool
    reasons: list[str]
    llm_attempted: bool
    llm_rejection_reason: str | None
    llm_suggested_category: str | None
    llm_suggested_confidence: float | None


class DuneHttpClient:
    def __init__(self, api_key: str, strict_no_execution: bool = True):
        self.api_key = api_key
        self.strict_no_execution = strict_no_execution
        self.call_counts: Counter[str] = Counter()

    def _ensure_allowed_endpoint(self, method: str, url: str) -> None:
        if not self.strict_no_execution:
            return
        parsed = urllib.parse.urlparse(url)
        endpoint = (method.upper(), parsed.path)

        if method.upper() == "GET" and re.fullmatch(r"/api/v1/queries", parsed.path):
            return
        if method.upper() == "GET" and re.fullmatch(r"/api/v1/query/\d+", parsed.path):
            return
        if method.upper() == "POST" and re.fullmatch(r"/api/v1/usage", parsed.path):
            return

        raise RuntimeError(
            f"Disallowed Dune endpoint in strict no-execution mode: {endpoint}. "
            "Allowed endpoints are GET /v1/queries, GET /v1/query/{id}, POST /v1/usage."
        )

    def _request_json(
        self,
        url: str,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        self._ensure_allowed_endpoint(method, url)
        body: bytes | None = None
        headers = {
            "X-Dune-Api-Key": self.api_key,
            "Accept": "application/json",
            "User-Agent": "balancer-dune-sync/1.0",
        }
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code} for {url}: {body_text}") from exc

        parsed = urllib.parse.urlparse(url)
        path = parsed.path
        if re.fullmatch(r"/api/v1/queries", path):
            self.call_counts["dune_list_queries"] += 1
        elif re.fullmatch(r"/api/v1/query/\d+", path):
            self.call_counts["dune_get_query"] += 1
        elif re.fullmatch(r"/api/v1/usage", path):
            self.call_counts["dune_usage"] += 1
        else:
            self.call_counts["dune_other"] += 1

        return data

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
            payload = self._request_json(url, method="GET")
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
                updated_at = recursive_get_first_datetime(
                    item,
                    ("updated_at", "updatedAt", "last_modified_at"),
                )
                dashboard_id = recursive_get_first_int(item, ("dashboard_id", "dashboardId"))
                dashboard_slug = recursive_get_first_str(item, ("dashboard_slug", "dashboardSlug"))
                out.append(
                    QueryMetadata(
                        query_id=qid,
                        name=name,
                        created_at=created_at,
                        updated_at=updated_at,
                        owner=owner,
                        dashboard_id=dashboard_id,
                        dashboard_slug=dashboard_slug,
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
        payload = self._request_json(f"{DUNE_API_BASE_URL}/query/{query_id}", method="GET")
        if not isinstance(payload, (dict, list)):
            raise RuntimeError(f"Unexpected query payload type for {query_id}: {type(payload).__name__}")

        name = recursive_get_first_str(payload, ("name", "query_name", "title")) or f"query_{query_id}"
        sql = recursive_get_first_str(payload, ("query_sql", "sql"))
        if not sql:
            raise RuntimeError(f"No SQL field found for query {query_id}")

        return QueryContent(query_id=query_id, name=name, sql=sql)

    @staticmethod
    def _parse_usage_snapshot(payload: dict[str, Any] | list[Any]) -> UsageSnapshot | None:
        if not isinstance(payload, dict):
            return None

        periods = payload.get("billing_periods")
        if not isinstance(periods, list):
            periods = payload.get("billingPeriods")
        if not isinstance(periods, list) or not periods:
            return None

        best = None
        best_start = None
        for period in periods:
            if not isinstance(period, dict):
                continue
            start = period.get("start_date")
            start_dt = parse_iso8601(f"{start}T00:00:00Z") if isinstance(start, str) else None
            if best is None or (start_dt and best_start and start_dt > best_start):
                best = period
                best_start = start_dt

        if not isinstance(best, dict):
            best = periods[-1] if isinstance(periods[-1], dict) else None
        if not isinstance(best, dict):
            return None

        credits_used = best.get("credits_used")
        if isinstance(credits_used, str):
            try:
                credits_used = float(credits_used)
            except ValueError:
                credits_used = None
        if not isinstance(credits_used, (int, float)):
            return None

        service_credits: dict[str, float] = {}
        service_candidates: list[dict[str, Any]] = []
        raw_services = best.get("services")
        if isinstance(raw_services, list):
            service_candidates = [x for x in raw_services if isinstance(x, dict)]
        if not service_candidates:
            raw_services = best.get("service_usages")
            if isinstance(raw_services, list):
                service_candidates = [x for x in raw_services if isinstance(x, dict)]

        for service in service_candidates:
            key = service.get("name") or service.get("service") or service.get("service_name")
            if not isinstance(key, str) or not key.strip():
                continue
            value = (
                service.get("credits_used")
                if service.get("credits_used") is not None
                else service.get("credits")
            )
            if isinstance(value, str):
                try:
                    value = float(value)
                except ValueError:
                    value = None
            if isinstance(value, (int, float)):
                service_credits[key] = float(value)

        return UsageSnapshot(
            credits_used=float(credits_used),
            billing_start=best.get("start_date") if isinstance(best.get("start_date"), str) else None,
            billing_end=best.get("end_date") if isinstance(best.get("end_date"), str) else None,
            service_credits=service_credits,
        )

    def get_usage_snapshot(self, start_date: str | None = None, end_date: str | None = None) -> UsageSnapshot | None:
        payload: dict[str, Any] = {}
        if start_date:
            payload["start_date"] = start_date
        if end_date:
            payload["end_date"] = end_date
        result = self._request_json(f"{DUNE_API_BASE_URL}/usage", method="POST", payload=payload)
        return self._parse_usage_snapshot(result)


class FixtureDuneClient:
    def __init__(self, fixtures_dir: Path):
        self.fixtures_dir = fixtures_dir
        self.call_counts: Counter[str] = Counter()

    def list_queries(self, limit: int = 100) -> list[QueryMetadata]:
        pages = sorted(self.fixtures_dir.glob("list_queries_page*.json"))
        if not pages:
            single = self.fixtures_dir / "list_queries.json"
            if not single.exists():
                raise FileNotFoundError(f"Fixture not found: {single}")
            pages = [single]

        helper = DuneHttpClient(api_key="fixture")
        out: list[QueryMetadata] = []
        seen = set()
        for page in pages:
            self.call_counts["dune_list_queries"] += 1
            payload = json.loads(page.read_text(encoding="utf-8"))
            items = helper._extract_items(payload)
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
                dashboard_id = recursive_get_first_int(item, ("dashboard_id", "dashboardId"))
                dashboard_slug = recursive_get_first_str(item, ("dashboard_slug", "dashboardSlug"))
                out.append(
                    QueryMetadata(
                        query_id=qid,
                        name=name,
                        created_at=created_at,
                        updated_at=updated_at,
                        owner=owner,
                        dashboard_id=dashboard_id,
                        dashboard_slug=dashboard_slug,
                    )
                )
        return out

    def get_query(self, query_id: int) -> QueryContent:
        self.call_counts["dune_get_query"] += 1
        fixture = self.fixtures_dir / f"query_{query_id}.json"
        if not fixture.exists():
            raise FileNotFoundError(f"Fixture not found: {fixture}")
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        name = recursive_get_first_str(payload, ("name", "query_name", "title")) or f"query_{query_id}"
        sql = recursive_get_first_str(payload, ("query_sql", "sql"))
        if not sql:
            raise RuntimeError(f"Fixture query payload for {query_id} has no SQL field")
        return QueryContent(query_id=query_id, name=name, sql=sql)

    def get_usage_snapshot(self, start_date: str | None = None, end_date: str | None = None) -> UsageSnapshot | None:
        label = "usage.json"
        if start_date or end_date:
            label = "usage_window.json"
        fixture = self.fixtures_dir / label
        if not fixture.exists():
            return None
        self.call_counts["dune_usage"] += 1
        payload = json.loads(fixture.read_text(encoding="utf-8"))
        return DuneHttpClient._parse_usage_snapshot(payload)


class OpenAILLMClassifier:
    def __init__(self, api_key: str, model: str, endpoint: str = "https://api.openai.com/v1/chat/completions"):
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint
        self.call_count = 0

    def classify(
        self,
        query_name: str,
        query_sql: str,
        taxonomy: list[str],
        top_candidates: list[tuple[str, float]],
        reasons: list[str],
    ) -> tuple[str | None, float | None, str | None]:
        self.call_count += 1
        sql_excerpt = query_sql[:1800]
        payload = {
            "model": self.model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You classify Dune SQL queries into a fixed Balancer repo taxonomy. "
                        "Return JSON only with keys: category, confidence, rationale. "
                        "Category must be one of the provided taxonomy entries or 'unclassified'. "
                        "Important: around 2% of query titles may be in Portuguese."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(
                        {
                            "query_name": query_name,
                            "query_sql_excerpt": sql_excerpt,
                            "taxonomy": taxonomy,
                            "deterministic_top_candidates": top_candidates,
                            "deterministic_reasons": reasons,
                        },
                        ensure_ascii=True,
                    ),
                },
            ],
            "temperature": 0,
        }

        req = urllib.request.Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "balancer-dune-sync/1.0",
            },
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            return None, None, f"llm_error:{type(exc).__name__}"

        content = None
        try:
            content = body["choices"][0]["message"]["content"]
        except Exception:
            return None, None, "llm_error:bad_response"
        if not isinstance(content, str):
            return None, None, "llm_error:no_content"

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.S)
            if not match:
                return None, None, "llm_error:invalid_json"
            try:
                parsed = json.loads(match.group(0))
            except json.JSONDecodeError:
                return None, None, "llm_error:invalid_json"

        category = parsed.get("category")
        confidence = parsed.get("confidence")
        rationale = parsed.get("rationale")
        if isinstance(confidence, str):
            try:
                confidence = float(confidence)
            except ValueError:
                confidence = None
        if not isinstance(category, str):
            category = None
        if not isinstance(confidence, (float, int)):
            confidence = None
        if not isinstance(rationale, str):
            rationale = None
        return category, float(confidence) if confidence is not None else None, rationale


def row_matches_owner_allowlist(row: QueryMetadata, allowlist: list[str], require_match: bool) -> bool:
    if not allowlist:
        return True
    owner_text = (row.owner or "").lower()
    if not owner_text:
        return not require_match
    return any(token.lower() in owner_text for token in allowlist)


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


def update_manifest_file(
    queries_yml_path: Path,
    append_entries: list[tuple[int, str]],
    category_updates: dict[int, str],
    dry_run: bool,
) -> tuple[bool, list[str]]:
    raw = queries_yml_path.read_text(encoding="utf-8")
    lines = raw.splitlines(keepends=True)
    changes: list[str] = []

    id_line_re = re.compile(r"^(\s*)-\s+id:\s*(\d+)\s*$")
    category_line_re = re.compile(r"^(\s*)category:\s*(.+?)\s*$")

    i = 0
    while i < len(lines):
        match = id_line_re.match(lines[i].rstrip("\n"))
        if not match:
            i += 1
            continue
        indent = match.group(1)
        qid = int(match.group(2))
        if qid not in category_updates:
            i += 1
            continue

        desired = category_updates[qid]
        j = i + 1
        found_category_line = None
        while j < len(lines):
            stripped = lines[j].lstrip(" ")
            if not lines[j].startswith(indent + "  "):
                break
            if category_line_re.match(lines[j].strip()):
                found_category_line = j
                break
            if stripped.startswith("- id:") or stripped.startswith("- "):
                break
            j += 1

        new_line = f"{indent}  category: {desired}\n"
        if found_category_line is not None:
            old = lines[found_category_line]
            if old != new_line:
                lines[found_category_line] = new_line
                changes.append(f"manifest_update:{qid}:{desired}")
        else:
            lines.insert(i + 1, new_line)
            changes.append(f"manifest_update:{qid}:{desired}")
            i += 1
        i += 1

    if append_entries:
        additions = ""
        for qid, cat in append_entries:
            additions += f"\n  - id: {qid}\n    category: {cat}\n"
            changes.append(f"manifest_append:{qid}:{cat}")
        lines.append(additions)

    new_text = "".join(lines)
    changed = new_text != raw
    if changed and not dry_run:
        queries_yml_path.write_text(new_text, encoding="utf-8")

    return changed, changes


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


def list_taxonomy_categories(repo_root: Path, include_unclassified: bool = True) -> set[str]:
    balancer_root = repo_root / "balancer"
    categories: set[str] = set()
    for p in balancer_root.rglob("*"):
        if p.is_dir():
            rel = p.relative_to(balancer_root)
            if not rel.parts:
                continue
            categories.add(rel.as_posix())
    if include_unclassified:
        categories.add("unclassified")
    categories.add("support/legacy")
    return categories


def load_yaml_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        return data
    return {}


def parse_dashboard_map(path: Path) -> dict[str, Any]:
    data = load_yaml_or_empty(path)
    out = {
        "query_overrides": {},
        "dashboard_overrides": {},
    }

    raw_query_overrides = data.get("query_overrides")
    if isinstance(raw_query_overrides, dict):
        for key, value in raw_query_overrides.items():
            try:
                qid = int(key)
            except (TypeError, ValueError):
                continue
            if isinstance(value, str):
                out["query_overrides"][qid] = value

    raw_dashboard = data.get("dashboard_overrides")
    if isinstance(raw_dashboard, dict):
        for key, value in raw_dashboard.items():
            if isinstance(key, str) and isinstance(value, str):
                out["dashboard_overrides"][key.lower()] = value

    return out


def load_classification_rules(path: Path) -> dict[str, Any]:
    data = load_yaml_or_empty(path)
    settings = data.get("settings") if isinstance(data.get("settings"), dict) else {}
    signal_weights = data.get("signal_weights") if isinstance(data.get("signal_weights"), dict) else {}

    result = {
        "settings": {
            "high_confidence_threshold": float(settings.get("high_confidence_threshold", 0.78)),
            "min_margin": float(settings.get("min_margin", 0.18)),
            "min_signal_votes": int(settings.get("min_signal_votes", 2)),
            "require_sql_or_dashboard": parse_bool_env(
                str(settings.get("require_sql_or_dashboard", "true")),
                default=True,
            ),
            "legacy_inactive_days": int(settings.get("legacy_inactive_days", DEFAULT_LEGACY_DAYS)),
            "llm_confidence_threshold": float(settings.get("llm_confidence_threshold", 0.78)),
            "llm_top_k_candidates": int(settings.get("llm_top_k_candidates", 3)),
        },
        "signal_weights": {
            "name": float(signal_weights.get("name", 0.35)),
            "sql": float(signal_weights.get("sql", 0.45)),
            "dashboard": float(signal_weights.get("dashboard", 0.20)),
        },
        "name_rules": [],
        "sql_rules": [],
        "special_name_rules": [],
    }

    for key in ("name_rules", "sql_rules", "special_name_rules"):
        raw = data.get(key)
        if isinstance(raw, list):
            cleaned = []
            for item in raw:
                if not isinstance(item, dict):
                    continue
                if not isinstance(item.get("pattern"), str):
                    continue
                if key != "special_name_rules" and not isinstance(item.get("category"), str):
                    continue
                cleaned.append(item)
            result[key] = cleaned

    return result


def is_inactive(meta: QueryMetadata, inactive_days: int, now: dt.datetime) -> bool:
    last = meta.updated_at or meta.created_at
    if not last:
        return False
    return last <= now - dt.timedelta(days=inactive_days)


def score_rules_against_text(
    text: str,
    rules: list[dict[str, Any]],
    signal_name: str,
) -> tuple[dict[str, float], dict[str, list[str]], dict[str, float]]:
    per_category: defaultdict[str, float] = defaultdict(float)
    reasons: defaultdict[str, list[str]] = defaultdict(list)
    signal_score: defaultdict[str, float] = defaultdict(float)

    for rule in rules:
        pattern = rule.get("pattern")
        category = rule.get("category")
        if not isinstance(pattern, str) or not isinstance(category, str):
            continue
        flags = re.I if parse_bool_env(str(rule.get("case_insensitive", "true")), default=True) else 0
        if not re.search(pattern, text, flags=flags):
            continue
        rule_weight = float(rule.get("weight", 1.0))
        per_category[category] += rule_weight
        signal_score[category] = min(1.0, signal_score[category] + rule_weight)
        reasons[category].append(f"{signal_name}:{pattern}")

    return dict(per_category), dict(reasons), dict(signal_score)


def classify_query(
    meta: QueryMetadata,
    query_sql: str,
    taxonomy: set[str],
    rules: dict[str, Any],
    dashboard_map: dict[str, Any],
    llm_classifier: OpenAILLMClassifier | None,
    llm_calls_used: int,
    llm_max_calls: int | None,
    llm_enabled: bool,
) -> tuple[ClassificationResult, int]:
    settings = rules["settings"]
    weights = rules["signal_weights"]

    normalized_name = meta.name.lower()
    normalized_sql = query_sql.lower()

    # Special deterministic rules (tests etc).
    for rule in rules["special_name_rules"]:
        pattern = rule.get("pattern")
        category = rule.get("category")
        if not isinstance(pattern, str) or not isinstance(category, str):
            continue
        if category not in taxonomy:
            continue
        flags = re.I if parse_bool_env(str(rule.get("case_insensitive", "true")), default=True) else 0
        if re.search(pattern, normalized_name, flags=flags):
            return (
                ClassificationResult(
                    category=category,
                    method="deterministic-special",
                    confidence=1.0,
                    top_category=category,
                    top_score=1.0,
                    second_category=None,
                    second_score=0.0,
                    votes=["name"],
                    used_llm=False,
                    reasons=[f"special:{pattern}"],
                    llm_attempted=False,
                    llm_rejection_reason=None,
                    llm_suggested_category=None,
                    llm_suggested_confidence=None,
                ),
                llm_calls_used,
            )

    raw_name_scores, name_reasons, name_signal = score_rules_against_text(
        normalized_name,
        rules["name_rules"],
        "name",
    )
    raw_sql_scores, sql_reasons, sql_signal = score_rules_against_text(
        normalized_sql,
        rules["sql_rules"],
        "sql",
    )

    dashboard_scores: defaultdict[str, float] = defaultdict(float)
    dashboard_signal: defaultdict[str, float] = defaultdict(float)
    dashboard_reasons: defaultdict[str, list[str]] = defaultdict(list)

    query_override = dashboard_map["query_overrides"].get(meta.query_id)
    if isinstance(query_override, str):
        dashboard_scores[query_override] += 1.0
        dashboard_signal[query_override] = 1.0
        dashboard_reasons[query_override].append("dashboard:query_override")

    for key in (
        str(meta.dashboard_id) if meta.dashboard_id is not None else None,
        meta.dashboard_slug.lower() if isinstance(meta.dashboard_slug, str) else None,
    ):
        if not key:
            continue
        category = dashboard_map["dashboard_overrides"].get(key)
        if isinstance(category, str):
            dashboard_scores[category] += 1.0
            dashboard_signal[category] = 1.0
            dashboard_reasons[category].append(f"dashboard:override:{key}")

    combined_scores: defaultdict[str, float] = defaultdict(float)
    signal_votes: defaultdict[str, set[str]] = defaultdict(set)
    reasons: defaultdict[str, list[str]] = defaultdict(list)

    for category, score in raw_name_scores.items():
        if category in taxonomy:
            combined_scores[category] += weights["name"] * min(1.0, score)
    for category, score in raw_sql_scores.items():
        if category in taxonomy:
            combined_scores[category] += weights["sql"] * min(1.0, score)
    for category, score in dashboard_scores.items():
        if category in taxonomy:
            combined_scores[category] += weights["dashboard"] * min(1.0, score)

    for category in set(combined_scores.keys()):
        if name_signal.get(category, 0.0) > 0:
            signal_votes[category].add("name")
            reasons[category].extend(name_reasons.get(category, []))
        if sql_signal.get(category, 0.0) > 0:
            signal_votes[category].add("sql")
            reasons[category].extend(sql_reasons.get(category, []))
        if dashboard_signal.get(category, 0.0) > 0:
            signal_votes[category].add("dashboard")
            reasons[category].extend(dashboard_reasons.get(category, []))

    sorted_scores = sorted(combined_scores.items(), key=lambda x: x[1], reverse=True)
    if sorted_scores:
        top_category, top_score = sorted_scores[0]
    else:
        top_category, top_score = "unclassified", 0.0

    second_category = sorted_scores[1][0] if len(sorted_scores) > 1 else None
    second_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0
    margin = top_score - second_score

    top_votes = sorted(signal_votes.get(top_category, set()))
    has_required_signal = ("sql" in top_votes) or ("dashboard" in top_votes)
    is_high_confidence = (
        top_category in taxonomy
        and top_score >= settings["high_confidence_threshold"]
        and margin >= settings["min_margin"]
        and len(top_votes) >= settings["min_signal_votes"]
        and (has_required_signal or not settings["require_sql_or_dashboard"])
    )

    if is_high_confidence:
        return (
            ClassificationResult(
                category=top_category,
                method="deterministic",
                confidence=float(top_score),
                top_category=top_category,
                top_score=float(top_score),
                second_category=second_category,
                second_score=float(second_score),
                votes=top_votes,
                used_llm=False,
                reasons=reasons.get(top_category, []),
                llm_attempted=False,
                llm_rejection_reason=None,
                llm_suggested_category=None,
                llm_suggested_confidence=None,
            ),
            llm_calls_used,
        )

    llm_attempted = False
    llm_rejection_reason: str | None = None
    llm_suggested_category: str | None = None
    llm_suggested_confidence: float | None = None

    llm_has_capacity = llm_max_calls is None or llm_calls_used < llm_max_calls

    if (
        llm_enabled
        and llm_classifier is not None
        and llm_has_capacity
        and sorted_scores
    ):
        candidate_list = [(c, round(s, 4)) for c, s in sorted_scores[: settings["llm_top_k_candidates"]]]
        top_candidate_categories = {c for c, _score in candidate_list}
        llm_category, llm_confidence, llm_rationale = llm_classifier.classify(
            query_name=meta.name,
            query_sql=query_sql,
            taxonomy=sorted(taxonomy),
            top_candidates=candidate_list,
            reasons=reasons.get(top_category, []),
        )
        llm_attempted = True
        llm_suggested_category = llm_category if isinstance(llm_category, str) else None
        llm_suggested_confidence = llm_confidence if isinstance(llm_confidence, float) else None
        llm_calls_used += 1
        if (
            isinstance(llm_category, str)
            and llm_category in taxonomy.union({"unclassified"})
            and isinstance(llm_confidence, float)
            and llm_confidence >= settings["llm_confidence_threshold"]
            and (llm_category in top_candidate_categories or top_score >= 0.30)
        ):
            llm_votes = copy.deepcopy(top_votes)
            llm_votes.append("llm")
            return (
                ClassificationResult(
                    category=llm_category,
                    method="llm",
                    confidence=llm_confidence,
                    top_category=top_category,
                    top_score=float(top_score),
                    second_category=second_category,
                    second_score=float(second_score),
                    votes=llm_votes,
                    used_llm=True,
                    reasons=[llm_rationale] if isinstance(llm_rationale, str) else ["llm:accepted"],
                    llm_attempted=True,
                    llm_rejection_reason=None,
                    llm_suggested_category=llm_category,
                    llm_suggested_confidence=llm_confidence,
                ),
                llm_calls_used,
            )
        if isinstance(llm_rationale, str) and llm_rationale.startswith("llm_error:"):
            llm_rejection_reason = llm_rationale
        elif not isinstance(llm_category, str):
            llm_rejection_reason = "llm_reject:invalid_category_type"
        elif llm_category not in taxonomy.union({"unclassified"}):
            llm_rejection_reason = f"llm_reject:category_not_in_taxonomy:{llm_category}"
        elif not isinstance(llm_confidence, float):
            llm_rejection_reason = "llm_reject:invalid_confidence_type"
        elif llm_confidence < settings["llm_confidence_threshold"]:
            llm_rejection_reason = (
                f"llm_reject:low_confidence:{round(llm_confidence, 4)}"
                f"<{settings['llm_confidence_threshold']}"
            )
        elif llm_category not in top_candidate_categories and top_score < 0.30:
            llm_rejection_reason = (
                "llm_reject:not_in_top_candidates_and_low_top_score:"
                f"{round(top_score, 4)}<0.30"
            )
        else:
            llm_rejection_reason = "llm_reject:unknown"
    else:
        if not llm_enabled:
            llm_rejection_reason = "llm_skipped:disabled"
        elif llm_classifier is None:
            llm_rejection_reason = "llm_skipped:no_classifier"
        elif llm_max_calls is not None and llm_calls_used >= llm_max_calls:
            llm_rejection_reason = "llm_skipped:max_calls_reached"
        elif not sorted_scores:
            llm_rejection_reason = "llm_skipped:no_candidates"

    return (
        ClassificationResult(
            category="unclassified",
            method="fallback-unclassified",
            confidence=float(top_score),
            top_category=top_category,
            top_score=float(top_score),
            second_category=second_category,
            second_score=float(second_score),
            votes=top_votes,
            used_llm=False,
            reasons=reasons.get(top_category, []) if top_category in reasons else ["low-confidence"],
            llm_attempted=llm_attempted,
            llm_rejection_reason=llm_rejection_reason,
            llm_suggested_category=llm_suggested_category,
            llm_suggested_confidence=llm_suggested_confidence,
        ),
        llm_calls_used,
    )


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
            f.write(f"sync_classified_count={summary.get('classified_count', 0)}\n")
            f.write(f"sync_unclassified_count={summary.get('unclassified_count', 0)}\n")


def move_file(old_path: Path, new_path: Path, dry_run: bool) -> bool:
    if old_path.resolve() == new_path.resolve():
        return False
    if dry_run:
        print(f"DRY-RUN MOVE {old_path} -> {new_path}")
        return True

    new_path.parent.mkdir(parents=True, exist_ok=True)
    if new_path.exists():
        old_text = old_path.read_text(encoding="utf-8") if old_path.exists() else ""
        new_text = new_path.read_text(encoding="utf-8")
        if old_text != new_text:
            new_path.write_text(old_text, encoding="utf-8")
        if old_path.exists():
            old_path.unlink()
        return True

    new_path.parent.mkdir(parents=True, exist_ok=True)
    old_path.rename(new_path)
    return True


def usage_service_delta(before: UsageSnapshot | None, after: UsageSnapshot | None) -> dict[str, float] | None:
    if not before or not after:
        return None
    services = set(before.service_credits.keys()) | set(after.service_credits.keys())
    delta: dict[str, float] = {}
    for service in sorted(services):
        change = after.service_credits.get(service, 0.0) - before.service_credits.get(service, 0.0)
        delta[service] = round(change, 6)
    return delta


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
    rules_path: Path | None = None,
    dashboard_map_path: Path | None = None,
    legacy_inactive_days: int = DEFAULT_LEGACY_DAYS,
    llm_classifier: OpenAILLMClassifier | None = None,
    llm_enabled: bool = False,
    llm_max_calls: int | None = None,
) -> dict[str, Any]:
    queries_yml = repo_root / "queries.yml"
    if not queries_yml.exists():
        raise FileNotFoundError(f"queries.yml not found at {queries_yml}")

    _, manifest_map = load_manifest_entries(queries_yml)
    manifest_ids = set(manifest_map.keys())

    if rules_path is None:
        rules_path = repo_root / DEFAULT_CLASSIFICATION_RULES
    if dashboard_map_path is None:
        dashboard_map_path = repo_root / DEFAULT_DASHBOARD_MAP

    taxonomy = list_taxonomy_categories(repo_root)
    rules = load_classification_rules(rules_path)
    dashboard_map = parse_dashboard_map(dashboard_map_path)

    list_rows = client.list_queries(limit=100)
    filtered_rows = [
        row for row in list_rows if row_matches_owner_allowlist(row, owner_allowlist, require_owner_match)
    ]
    filtered_ids = {row.query_id for row in filtered_rows}
    excluded_by_owner_ids = sorted(row.query_id for row in list_rows if row.query_id not in filtered_ids)
    dune_by_id = {row.query_id: row for row in filtered_rows}
    dune_ids = set(dune_by_id.keys())
    now = utc_now()

    previous_success = get_previous_success_timestamp(
        repo_slug=os.environ.get("GITHUB_REPOSITORY"),
        github_token=os.environ.get("GITHUB_TOKEN"),
        current_run_id=os.environ.get("GITHUB_RUN_ID"),
        workflow_file=workflow_file,
    )
    anchor = previous_success or now
    watermark = anchor - dt.timedelta(hours=lookback_hours)

    legacy_days = legacy_inactive_days or rules["settings"]["legacy_inactive_days"]

    usage_before = None
    usage_after = None
    try:
        usage_before = client.get_usage_snapshot()
    except Exception as exc:
        print(f"WARNING: usage snapshot before run failed ({type(exc).__name__}: {exc})")

    new_ids = sorted(dune_ids - manifest_ids)
    candidate_updated_ids: list[int] = []
    for qid in sorted(dune_ids & manifest_ids):
        updated_at = dune_by_id[qid].updated_at
        if updated_at and updated_at > watermark:
            candidate_updated_ids.append(qid)

    unclassified_manifest_ids = sorted(
        qid for qid, category in manifest_map.items() if category == "unclassified" and qid in dune_ids
    )

    candidate_ids = sorted(set(new_ids) | set(candidate_updated_ids) | set(unclassified_manifest_ids))

    changed_files: list[str] = []
    new_files: list[str] = []
    updated_files: list[str] = []
    moved_to_legacy_files: list[str] = []
    append_entries: list[tuple[int, str]] = []
    manifest_category_updates: dict[int, str] = {}
    failed_ids: list[int] = []
    skipped_ids: list[int] = []
    unchanged_ids: list[int] = []
    classification_details: dict[int, dict[str, Any]] = {}
    low_confidence_ids: list[int] = []
    legacy_candidate_ids: list[int] = []
    llm_calls_used = 0
    effective_llm_max_calls = (
        llm_max_calls if isinstance(llm_max_calls, int) and llm_max_calls > 0 else None
    )

    print(
        f"Listed {len(list_rows)} queries from Dune "
        f"(owner-filtered to {len(filtered_rows)}). "
        f"new_ids={len(new_ids)} updated_candidates={len(candidate_updated_ids)} "
        f"legacy_review={len(unclassified_manifest_ids)} watermark={watermark.isoformat()}"
    )

    for qid in candidate_ids:
        meta = dune_by_id.get(qid)
        if meta is None:
            skipped_ids.append(qid)
            continue

        is_new = qid in new_ids
        existing_category = manifest_map.get(qid)
        is_existing_unclassified = (not is_new) and existing_category == "unclassified"

        # Existing unclassified stale queries can be moved to support/legacy without SQL fetch.
        if is_existing_unclassified and qid not in candidate_updated_ids:
            if is_inactive(meta, legacy_days, now):
                legacy_candidate_ids.append(qid)
                existing_file = find_existing_file(qid, repo_root)
                if existing_file is None:
                    skipped_ids.append(qid)
                    continue
                target = repo_root / "balancer" / "support" / "legacy" / existing_file.name
                changed = move_file(existing_file, target, dry_run=dry_run)
                if changed:
                    rel = str(target.relative_to(repo_root))
                    changed_files.append(rel)
                    moved_to_legacy_files.append(rel)
                    manifest_category_updates[qid] = "support/legacy"
                    print(f"LEGACY MOVE: query {qid} -> support/legacy")
                else:
                    unchanged_ids.append(qid)
            continue

        try:
            query = client.get_query(qid)
        except Exception as exc:
            print(f"SKIP {qid}: failed to fetch query ({type(exc).__name__}: {exc})")
            failed_ids.append(qid)
            continue

        if is_new:
            result, llm_calls_used = classify_query(
                meta=meta,
                query_sql=query.sql,
                taxonomy=taxonomy,
                rules=rules,
                dashboard_map=dashboard_map,
                llm_classifier=llm_classifier,
                llm_calls_used=llm_calls_used,
                llm_max_calls=effective_llm_max_calls,
                llm_enabled=llm_enabled,
            )

            assigned_category = result.category if result.category in taxonomy else default_new_category

            if assigned_category == "unclassified":
                low_confidence_ids.append(qid)

            if is_inactive(meta, legacy_days, now):
                assigned_category = "support/legacy"
                legacy_candidate_ids.append(qid)
                result.reasons.append(f"legacy:{legacy_days}d")

            classification_details[qid] = {
                "category": assigned_category,
                "method": result.method,
                "confidence": round(result.confidence, 4),
                "top_category": result.top_category,
                "top_score": round(result.top_score, 4),
                "second_category": result.second_category,
                "second_score": round(result.second_score, 4),
                "votes": result.votes,
                "reasons": result.reasons,
                "used_llm": result.used_llm,
                "llm_attempted": result.llm_attempted,
                "llm_rejection_reason": result.llm_rejection_reason,
                "llm_suggested_category": result.llm_suggested_category,
                "llm_suggested_confidence": (
                    round(result.llm_suggested_confidence, 4)
                    if isinstance(result.llm_suggested_confidence, float)
                    else None
                ),
            }

            target_dir = repo_root / "balancer" / assigned_category
            file_path = target_dir / f"{sanitize_name(query.name)}_{qid}.sql"
            final_sql = ensure_sql_with_header(query.sql, query.name, qid)
            existing = find_existing_file(qid, repo_root)

            changed = False
            if existing is None:
                changed = True
                if dry_run:
                    print(f"DRY-RUN CREATE {file_path}")
                else:
                    target_dir.mkdir(parents=True, exist_ok=True)
                    file_path.write_text(final_sql, encoding="utf-8")
            else:
                if existing.resolve() != file_path.resolve():
                    moved = move_file(existing, file_path, dry_run=dry_run)
                    changed = changed or moved
                current_text = existing.read_text(encoding="utf-8") if existing.exists() else ""
                if current_text != final_sql:
                    changed = True
                    if dry_run:
                        print(f"DRY-RUN UPDATE {file_path}")
                    else:
                        file_path.write_text(final_sql, encoding="utf-8")

            if changed:
                rel = str(file_path.relative_to(repo_root))
                changed_files.append(rel)
                new_files.append(rel)
            else:
                unchanged_ids.append(qid)

            append_entries.append((qid, assigned_category))
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

    append_entries = sorted(set(append_entries), key=lambda x: x[0])
    manifest_changed, manifest_changes = update_manifest_file(
        queries_yml_path=queries_yml,
        append_entries=append_entries,
        category_updates=manifest_category_updates,
        dry_run=dry_run,
    )
    if manifest_changed:
        changed_files.append("queries.yml")
        for c in manifest_changes:
            print(f"MANIFEST CHANGE: {c}")

    try:
        usage_after = client.get_usage_snapshot()
    except Exception as exc:
        print(f"WARNING: usage snapshot after run failed ({type(exc).__name__}: {exc})")

    usage_delta = None
    if usage_before and usage_after:
        usage_delta = round(usage_after.credits_used - usage_before.credits_used, 6)
    usage_service_delta_map = usage_service_delta(usage_before, usage_after)

    category_distribution = Counter([entry[1] for entry in append_entries])
    llm_rejections_by_reason = Counter(
        [
            details["llm_rejection_reason"]
            for details in classification_details.values()
            if isinstance(details.get("llm_rejection_reason"), str)
        ]
    )
    llm_attempted_ids = sorted(
        [int(qid) for qid, details in classification_details.items() if details.get("llm_attempted")]
    )

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
        "legacy_review_ids": unclassified_manifest_ids,
        "new_ids_appended_to_manifest": [x[0] for x in append_entries],
        "manifest_category_updates": manifest_category_updates,
        "changed_files": sorted(set(changed_files)),
        "new_files": sorted(set(new_files)),
        "updated_files": sorted(set(updated_files)),
        "moved_to_legacy_files": sorted(set(moved_to_legacy_files)),
        "failed_ids": sorted(set(failed_ids)),
        "skipped_ids": sorted(set(skipped_ids)),
        "unchanged_ids": sorted(set(unchanged_ids)),
        "legacy_candidate_ids": sorted(set(legacy_candidate_ids)),
        "classification_details": classification_details,
        "classified_count": len([1 for _qid, cat in append_entries if cat != "unclassified"]),
        "unclassified_count": len([1 for _qid, cat in append_entries if cat == "unclassified"]),
        "low_confidence_ids": sorted(set(low_confidence_ids)),
        "llm_fallback_count": len([1 for v in classification_details.values() if v.get("used_llm")]),
        "llm_attempted_count": len(llm_attempted_ids),
        "llm_attempted_ids": llm_attempted_ids,
        "llm_rejections_by_reason": dict(sorted(llm_rejections_by_reason.items())),
        "llm_calls_used": llm_calls_used,
        "llm_max_calls": effective_llm_max_calls,
        "category_distribution": dict(sorted(category_distribution.items())),
        "dune_call_counts": dict(client.call_counts),
        "usage_before": usage_before.__dict__ if usage_before else None,
        "usage_after": usage_after.__dict__ if usage_after else None,
        "usage_delta_credits": usage_delta,
        "usage_delta_services": usage_service_delta_map,
        "new_count": len(set(new_files)),
        "updated_count": len(set(updated_files)),
        "failed_count": len(set(failed_ids)),
        "changes_total": len(set(changed_files)),
        "strict_no_execution_mode": bool(getattr(client, "strict_no_execution", False)),
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
        help="Fallback category path under balancer/ for low-confidence new queries.",
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
        help="Drop rows with missing/unknown owner metadata when owner allowlist is provided.",
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
    parser.add_argument(
        "--classification-rules",
        default=DEFAULT_CLASSIFICATION_RULES,
        help=f"Path to classification rules YAML (default: {DEFAULT_CLASSIFICATION_RULES}).",
    )
    parser.add_argument(
        "--dashboard-map",
        default=DEFAULT_DASHBOARD_MAP,
        help=f"Path to dashboard map YAML (default: {DEFAULT_DASHBOARD_MAP}).",
    )
    parser.add_argument(
        "--legacy-inactive-days",
        type=int,
        default=int(os.environ.get("SYNC_LEGACY_INACTIVE_DAYS", str(DEFAULT_LEGACY_DAYS))),
        help=f"Inactive-day threshold for auto-legacy routing (default: {DEFAULT_LEGACY_DAYS}).",
    )
    parser.add_argument(
        "--llm-fallback-enabled",
        action="store_true",
        default=parse_bool_env(os.environ.get("SYNC_LLM_FALLBACK_ENABLED"), default=False),
        help="Enable LLM fallback classification for ambiguous queries.",
    )
    parser.add_argument(
        "--llm-max-calls",
        type=int,
        default=int(os.environ.get("SYNC_LLM_MAX_CALLS_PER_RUN", "0")),
        help="Maximum number of LLM fallback calls per run (<=0 means unlimited).",
    )
    parser.add_argument(
        "--llm-model",
        default=os.environ.get("SYNC_LLM_MODEL", "gpt-4o-mini"),
        help="Model to use for LLM fallback classification.",
    )
    parser.add_argument(
        "--strict-no-execution",
        action="store_true",
        default=parse_bool_env(os.environ.get("SYNC_STRICT_NO_EXECUTION"), default=True),
        help="Enforce strict endpoint allowlist (no execution/export/write Dune endpoints).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")

    fixtures_dir = Path(args.fixtures_dir).resolve() if args.fixtures_dir else None
    summary_path = (repo_root / args.summary_path).resolve()
    rules_path = (repo_root / args.classification_rules).resolve()
    dashboard_map_path = (repo_root / args.dashboard_map).resolve()

    llm_classifier = None
    if args.llm_fallback_enabled:
        llm_api_key = os.environ.get("OPENAI_API_KEY")
        if llm_api_key:
            llm_classifier = OpenAILLMClassifier(api_key=llm_api_key, model=args.llm_model)
        else:
            print("WARNING: LLM fallback enabled but OPENAI_API_KEY missing; fallback disabled.")

    if fixtures_dir:
        client: DuneHttpClient | FixtureDuneClient = FixtureDuneClient(fixtures_dir=fixtures_dir)
        print(f"Using fixtures from {fixtures_dir}")
    else:
        api_key = os.environ.get("DUNE_API_KEY")
        if not api_key:
            raise SystemExit("DUNE_API_KEY is required")
        if not os.environ.get("GITHUB_TOKEN"):
            print("WARNING: GITHUB_TOKEN missing; previous-success watermark lookup will be skipped.")
        if not os.environ.get("GITHUB_REPOSITORY"):
            print("WARNING: GITHUB_REPOSITORY missing; previous-success watermark lookup will be skipped.")
        client = DuneHttpClient(api_key=api_key, strict_no_execution=args.strict_no_execution)

    summary = sync_queries(
        repo_root=repo_root,
        client=client,
        lookback_hours=args.lookback_hours,
        dry_run=args.dry_run,
        default_new_category=args.new_category.strip("/"),
        owner_allowlist=[x.lower() for x in parse_csv_list(args.owner_allowlist)],
        require_owner_match=args.require_owner_match,
        workflow_file=args.workflow_file,
        summary_path=summary_path,
        rules_path=rules_path,
        dashboard_map_path=dashboard_map_path,
        legacy_inactive_days=args.legacy_inactive_days,
        llm_classifier=llm_classifier,
        llm_enabled=args.llm_fallback_enabled,
        llm_max_calls=(args.llm_max_calls if args.llm_max_calls > 0 else None),
    )

    if summary["failed_count"] > 0:
        print(
            "WARNING: Some queries failed to fetch. "
            f"failed_ids={','.join(str(x) for x in summary['failed_ids'])}"
        )


if __name__ == "__main__":
    main()
