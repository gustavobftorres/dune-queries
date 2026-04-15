"""
Propose classification rule updates from recent sync summaries.

This script is intentionally proposal-only: it never mutates runtime
classification rules automatically.
"""

from __future__ import annotations

import argparse
import io
import json
import math
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_WORKFLOW_FILE = "sync_from_dune.yml"
DEFAULT_RULES_PATH = "scripts/classification_rules.yml"
DEFAULT_OUTPUT_PATH = "scripts/classification_rule_proposals.yml"
SUMMARY_ARTIFACT_NAME = "sync-from-dune-summary"
MAX_RUNS_TO_SCAN = 120
DEFAULT_DAYS = 30

STOPWORDS = {
    "all",
    "analysis",
    "and",
    "balancer",
    "by",
    "daily",
    "data",
    "for",
    "from",
    "new",
    "on",
    "pool",
    "protocol",
    "query",
    "stats",
    "the",
    "total",
    "v2",
    "v3",
    "weekly",
}


@dataclass
class LearnerSettings:
    support_threshold: int
    precision_threshold: float
    stability_runs: int


def parse_iso8601(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    txt = value.strip()
    if not txt:
        return None
    try:
        if txt.endswith("Z"):
            return datetime.fromisoformat(txt.replace("Z", "+00:00"))
        parsed = datetime.fromisoformat(txt)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def load_settings(path: Path) -> LearnerSettings:
    if not path.exists():
        return LearnerSettings(8, 0.90, 2)
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    settings = payload.get("settings") if isinstance(payload, dict) else {}
    if not isinstance(settings, dict):
        settings = {}
    return LearnerSettings(
        support_threshold=int(settings.get("learner_support_threshold", 8)),
        precision_threshold=float(settings.get("learner_precision_threshold", 0.90)),
        stability_runs=int(settings.get("learner_stability_runs", 2)),
    )


def gh_request_json(url: str, token: str) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "balancer-rule-learner/1.0",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def gh_request_bytes(url: str, token: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "balancer-rule-learner/1.0",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def load_summary_from_zip(content: bytes) -> dict[str, Any] | None:
    with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".json"):
                continue
            raw = zf.read(name).decode("utf-8")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and "classification_details" in payload:
                return payload
    return None


def fetch_recent_summaries_from_github(
    repo: str,
    token: str,
    workflow_file: str,
    days: int,
    max_runs: int,
) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    page = 1
    summaries: list[dict[str, Any]] = []
    scanned = 0

    while scanned < max_runs:
        url = (
            f"https://api.github.com/repos/{repo}/actions/workflows/{workflow_file}/runs"
            f"?status=success&per_page=100&page={page}"
        )
        payload = gh_request_json(url, token)
        runs = payload.get("workflow_runs")
        if not isinstance(runs, list) or not runs:
            break

        for run in runs:
            if scanned >= max_runs:
                break
            scanned += 1
            run_created = parse_iso8601(run.get("created_at"))
            if run_created and run_created < cutoff:
                continue

            artifacts_url = run.get("artifacts_url")
            if not isinstance(artifacts_url, str) or not artifacts_url:
                continue

            try:
                artifacts_payload = gh_request_json(artifacts_url, token)
            except Exception:
                continue
            artifacts = artifacts_payload.get("artifacts")
            if not isinstance(artifacts, list):
                continue

            for artifact in artifacts:
                if not isinstance(artifact, dict):
                    continue
                if artifact.get("expired"):
                    continue
                if artifact.get("name") != SUMMARY_ARTIFACT_NAME:
                    continue
                archive_url = artifact.get("archive_download_url")
                if not isinstance(archive_url, str) or not archive_url:
                    continue
                try:
                    blob = gh_request_bytes(archive_url, token)
                    summary = load_summary_from_zip(blob)
                except Exception:
                    summary = None
                if summary:
                    summaries.append(summary)
                break

        page += 1

    return summaries


def load_local_summaries(path: Path, days: int) -> list[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    summaries: list[dict[str, Any]] = []
    for candidate in sorted(path.glob("*.json")):
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        ts = parse_iso8601(payload.get("timestamp_utc"))
        if ts and ts < cutoff:
            continue
        if "classification_details" in payload:
            summaries.append(payload)
    return summaries


def extract_slug_tokens(new_file: str) -> list[str]:
    filename = Path(new_file).name
    stem = filename[:-4] if filename.endswith(".sql") else filename
    stem = re.sub(r"_\d+$", "", stem)
    pieces = [p for p in stem.split("_") if p]
    return [p.lower() for p in pieces]


def is_noisy_token(token: str) -> bool:
    if len(token) <= 1:
        return True
    if token in STOPWORDS:
        return True
    if token in {"test", "teste"}:
        return True
    digits = sum(ch.isdigit() for ch in token)
    if digits > 0 and (digits / len(token)) >= 0.4:
        return True
    if not re.fullmatch(r"[a-z0-9]+", token):
        return True
    return False


def proposal_weight(precision: float, support: int) -> float:
    raw = 0.55 + (precision - 0.90) + min(0.25, math.log10(max(1, support)) * 0.08)
    return round(max(0.55, min(0.95, raw)), 2)


def build_rule_proposals(summaries: list[dict[str, Any]], settings: LearnerSettings) -> dict[str, Any]:
    token_counts: dict[str, Counter[str]] = defaultdict(Counter)
    token_run_buckets: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

    for idx, summary in enumerate(summaries):
        details = summary.get("classification_details")
        if not isinstance(details, dict):
            continue
        run_key = summary.get("timestamp_utc")
        if not isinstance(run_key, str) or not run_key:
            run_key = f"run-{idx}"

        slug_by_id: dict[str, str] = {}
        for path in summary.get("new_files", []) or []:
            if not isinstance(path, str):
                continue
            match = re.search(r"_(\d+)\.sql$", path)
            if not match:
                continue
            slug_by_id[match.group(1)] = path

        for qid_raw, entry in details.items():
            if not isinstance(entry, dict):
                continue
            category = entry.get("category")
            if not isinstance(category, str) or category == "unclassified":
                continue
            method = entry.get("method")
            if method == "fallback-unclassified":
                continue
            confidence = entry.get("confidence")
            if isinstance(confidence, (int, float)) and float(confidence) < 0.78:
                continue

            qid = str(qid_raw)
            file_path = slug_by_id.get(qid)
            if not file_path:
                continue
            tokens = extract_slug_tokens(file_path)
            for token in tokens:
                if is_noisy_token(token):
                    continue
                token_counts[token][category] += 1
                token_run_buckets[token][category].add(run_key)

    candidates: list[dict[str, Any]] = []
    for token, counts in token_counts.items():
        total = sum(counts.values())
        if total < settings.support_threshold:
            continue
        top_category, top_count = counts.most_common(1)[0]
        precision = top_count / total if total else 0.0
        if precision < settings.precision_threshold:
            continue
        stable_runs = len(token_run_buckets[token][top_category])
        if stable_runs < settings.stability_runs:
            continue
        candidates.append(
            {
                "pattern": rf"(^|[_\s-]){re.escape(token)}([_\s-]|$)",
                "category": top_category,
                "weight": proposal_weight(precision, total),
                "support": total,
                "estimated_precision": round(precision, 4),
                "stable_runs": stable_runs,
                "token": token,
            }
        )

    candidates.sort(key=lambda x: (-x["support"], -x["estimated_precision"], x["token"]))
    return {
        "settings": {
            "support_threshold": settings.support_threshold,
            "precision_threshold": settings.precision_threshold,
            "stability_runs": settings.stability_runs,
        },
        "candidate_count": len(candidates),
        "candidates": candidates,
    }


def write_if_changed(path: Path, payload: dict[str, Any]) -> bool:
    new_text = yaml.safe_dump(payload, sort_keys=False)
    old_text = path.read_text(encoding="utf-8") if path.exists() else None
    if old_text == new_text:
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(new_text, encoding="utf-8")
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Propose classification rule updates from sync summaries.")
    parser.add_argument("--summaries-dir", default=None, help="Local directory with summary JSON files.")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Lookback window in days.")
    parser.add_argument(
        "--workflow-file",
        default=DEFAULT_WORKFLOW_FILE,
        help=f"Workflow filename used for summary artifacts (default: {DEFAULT_WORKFLOW_FILE}).",
    )
    parser.add_argument(
        "--rules-path",
        default=DEFAULT_RULES_PATH,
        help=f"Classification rules path (default: {DEFAULT_RULES_PATH}).",
    )
    parser.add_argument(
        "--output-path",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Proposal output path (default: {DEFAULT_OUTPUT_PATH}).",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=MAX_RUNS_TO_SCAN,
        help=f"Max workflow runs to scan from GitHub API (default: {MAX_RUNS_TO_SCAN}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    settings = load_settings(repo_root / args.rules_path)

    if args.summaries_dir:
        summaries = load_local_summaries(Path(args.summaries_dir).resolve(), days=args.days)
    else:
        token = os.environ.get("GITHUB_TOKEN")
        repo = os.environ.get("GITHUB_REPOSITORY")
        if not token or not repo:
            raise SystemExit("Either --summaries-dir or both GITHUB_TOKEN and GITHUB_REPOSITORY are required.")
        summaries = fetch_recent_summaries_from_github(
            repo=repo,
            token=token,
            workflow_file=args.workflow_file,
            days=args.days,
            max_runs=args.max_runs,
        )

    if not summaries:
        print("No sync summaries found in lookback window.")
        return

    proposals = build_rule_proposals(summaries, settings=settings)
    proposals["source_summary_count"] = len(summaries)
    proposals["workflow_file"] = args.workflow_file
    proposals["lookback_days"] = args.days

    if proposals["candidate_count"] == 0:
        print("No rule proposals passed support/precision/stability gates.")
        return

    output_path = (repo_root / args.output_path).resolve()
    changed = write_if_changed(output_path, proposals)
    print(
        "Generated classification rule proposals: "
        f"{output_path} (candidates={proposals['candidate_count']} changed={changed})"
    )


if __name__ == "__main__":
    main()
