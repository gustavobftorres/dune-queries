from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


def load_sync_module():
    module_path = Path(__file__).resolve().parents[1] / "sync_from_dune_incremental.py"
    spec = importlib.util.spec_from_file_location("sync_from_dune_incremental", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync = load_sync_module()


class FakeLLM:
    def __init__(self, category: str, confidence: float):
        self.category = category
        self.confidence = confidence
        self.call_count = 0

    def classify(self, **_kwargs):
        self.call_count += 1
        return self.category, self.confidence, "fake-llm"


class SyncFromDuneIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sync-from-dune-test-")
        self.repo_root = Path(self.tmp) / "repo"
        self.repo_root.mkdir(parents=True, exist_ok=True)
        self.make_taxonomy()

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def make_taxonomy(self):
        dirs = [
            "balancer/unclassified",
            "balancer/metrics/volume/protocol",
            "balancer/metrics/volume/source",
            "balancer/metrics/fees/protocol",
            "balancer/analysis/comparisons",
            "balancer/support/legacy",
            "balancer/support/qa",
        ]
        for rel in dirs:
            (self.repo_root / rel).mkdir(parents=True, exist_ok=True)

    def write_manifest(self, entries):
        lines = ["query_ids:"]
        for entry in entries:
            lines.append(f"  - id: {entry['id']}")
            if entry.get("category"):
                lines.append(f"    category: {entry['category']}")
        (self.repo_root / "queries.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")

    def write_query_file(self, relative_path: str, query_id: int, name: str, sql: str):
        file_path = self.repo_root / relative_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        text = sync.build_header(name, query_id) + sql + "\n"
        file_path.write_text(text, encoding="utf-8")

    def write_fixture_files(self, fixture_payloads: dict[str, dict]):
        fixtures_dir = Path(self.tmp) / "fixtures"
        fixtures_dir.mkdir(parents=True, exist_ok=True)
        for filename, payload in fixture_payloads.items():
            (fixtures_dir / filename).write_text(json.dumps(payload), encoding="utf-8")
        return fixtures_dir

    def write_classifier_files(self, rules: dict | None = None, dashboard_map: dict | None = None):
        scripts_dir = self.repo_root / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)

        if rules is None:
            rules = {
                "settings": {
                    "high_confidence_threshold": 0.78,
                    "min_margin": 0.18,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [
                    {
                        "pattern": "(^|[^a-z])(test|teste)([^a-z]|$)",
                        "category": "support/legacy",
                        "case_insensitive": True,
                    }
                ],
                "name_rules": [],
                "sql_rules": [],
            }
        if dashboard_map is None:
            dashboard_map = {"dashboard_overrides": {}, "query_overrides": {}}

        (scripts_dir / "classification_rules.yml").write_text(
            json_to_yaml(rules),
            encoding="utf-8",
        )
        (scripts_dir / "dashboard_category_map.yml").write_text(
            json_to_yaml(dashboard_map),
            encoding="utf-8",
        )

    def run_sync(
        self,
        fixtures_dir: Path,
        dry_run: bool = False,
        owner_allowlist: list[str] | None = None,
        require_owner_match: bool = False,
        llm_classifier=None,
        llm_enabled: bool = False,
        llm_max_calls: int = 0,
    ):
        old_repo = os.environ.pop("GITHUB_REPOSITORY", None)
        old_token = os.environ.pop("GITHUB_TOKEN", None)
        old_run_id = os.environ.pop("GITHUB_RUN_ID", None)
        try:
            client = sync.FixtureDuneClient(fixtures_dir)
            summary_path = Path(self.tmp) / "summary.json"
            return sync.sync_queries(
                repo_root=self.repo_root,
                client=client,
                lookback_hours=30,
                dry_run=dry_run,
                default_new_category="unclassified",
                owner_allowlist=owner_allowlist or [],
                require_owner_match=require_owner_match,
                workflow_file="sync_from_dune.yml",
                summary_path=summary_path,
                rules_path=self.repo_root / "scripts" / "classification_rules.yml",
                dashboard_map_path=self.repo_root / "scripts" / "dashboard_category_map.yml",
                legacy_inactive_days=180,
                llm_classifier=llm_classifier,
                llm_enabled=llm_enabled,
                llm_max_calls=llm_max_calls,
            )
        finally:
            if old_repo is not None:
                os.environ["GITHUB_REPOSITORY"] = old_repo
            if old_token is not None:
                os.environ["GITHUB_TOKEN"] = old_token
            if old_run_id is not None:
                os.environ["GITHUB_RUN_ID"] = old_run_id

    def test_high_confidence_routes_to_taxonomy_folder(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.78,
                    "min_margin": 0.18,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [],
                "name_rules": [{"pattern": "volume_by_source", "category": "metrics/volume/source", "weight": 1.0}],
                "sql_rules": [{"pattern": "dex\\.trades", "category": "metrics/volume/source", "weight": 1.0}],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 200, "name": "Balancer volume_by_source", "updated_at": "2026-04-10T05:00:00Z"}]},
                "query_200.json": {"query_id": 200, "name": "Balancer volume_by_source", "query_sql": "select * from dex.trades"},
            }
        )

        summary = self.run_sync(fixtures)

        created = list((self.repo_root / "balancer" / "metrics" / "volume" / "source").glob("*_200.sql"))
        self.assertEqual(len(created), 1)
        self.assertEqual(summary["classification_details"][200]["category"], "metrics/volume/source")
        self.assertEqual(summary["classification_details"][200]["method"], "deterministic")

    def test_ambiguous_query_falls_back_to_unclassified(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.78,
                    "min_margin": 0.18,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [],
                "name_rules": [{"pattern": "volume", "category": "metrics/volume/protocol", "weight": 1.0}],
                "sql_rules": [{"pattern": "protocol_fees", "category": "metrics/fees/protocol", "weight": 1.0}],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 300, "name": "volume mixed", "updated_at": "2026-04-10T05:00:00Z"}]},
                "query_300.json": {"query_id": 300, "name": "volume mixed", "query_sql": "select * from protocol_fees"},
            }
        )

        summary = self.run_sync(fixtures)

        created = list((self.repo_root / "balancer" / "unclassified").glob("*_300.sql"))
        self.assertEqual(len(created), 1)
        self.assertEqual(summary["classification_details"][300]["category"], "unclassified")
        self.assertIn(300, summary["low_confidence_ids"])

    def test_test_or_teste_routes_to_legacy(self):
        self.write_manifest([])
        self.write_classifier_files()
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 310, "name": "teste materializada", "updated_at": "2026-04-10T05:00:00Z"}]},
                "query_310.json": {"query_id": 310, "name": "teste materializada", "query_sql": "select 1"},
            }
        )

        summary = self.run_sync(fixtures)
        created = list((self.repo_root / "balancer" / "support" / "legacy").glob("*_310.sql"))

        self.assertEqual(len(created), 1)
        self.assertEqual(summary["classification_details"][310]["category"], "support/legacy")

    def test_special_family_rules_route_matchorders_and_custom_lending(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.90,
                    "min_margin": 0.50,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [
                    {"pattern": "^matchorders?(?:_|$)", "category": "metrics/volume/source", "case_insensitive": True},
                    {"pattern": "^custom_lending(?:_|$)", "category": "support/qa", "case_insensitive": True},
                ],
                "name_rules": [],
                "sql_rules": [],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 330, "name": "matchorders_dex", "updated_at": "2026-04-10T05:00:00Z"},
                        {"id": 331, "name": "custom_lending_v2", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
                "query_330.json": {"query_id": 330, "name": "matchorders_dex", "query_sql": "select 1"},
                "query_331.json": {"query_id": 331, "name": "custom_lending_v2", "query_sql": "select 2"},
            }
        )

        summary = self.run_sync(fixtures)
        self.assertEqual(summary["classification_details"][330]["category"], "metrics/volume/source")
        self.assertEqual(summary["classification_details"][331]["category"], "support/qa")
        created_330 = list((self.repo_root / "balancer" / "metrics" / "volume" / "source").glob("*_330.sql"))
        created_331 = list((self.repo_root / "balancer" / "support" / "qa").glob("*_331.sql"))
        self.assertEqual(len(created_330), 1)
        self.assertEqual(len(created_331), 1)

    def test_legacy_180_days_applies_to_new_queries(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.78,
                    "min_margin": 0.18,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [],
                "name_rules": [{"pattern": "volume", "category": "metrics/volume/protocol", "weight": 1.0}],
                "sql_rules": [{"pattern": "dex\\.trades", "category": "metrics/volume/protocol", "weight": 1.0}],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 320, "name": "volume old", "updated_at": "2025-01-01T00:00:00Z"}]},
                "query_320.json": {"query_id": 320, "name": "volume old", "query_sql": "select * from dex.trades"},
            }
        )

        summary = self.run_sync(fixtures)
        created = list((self.repo_root / "balancer" / "support" / "legacy").glob("*_320.sql"))

        self.assertEqual(len(created), 1)
        self.assertIn(320, summary["legacy_candidate_ids"])
        self.assertEqual(summary["classification_details"][320]["category"], "support/legacy")

    def test_existing_unclassified_inactive_moves_to_legacy_without_fetch(self):
        self.write_manifest([{"id": 400, "category": "unclassified"}])
        self.write_classifier_files()
        rel = "balancer/unclassified/old_400.sql"
        self.write_query_file(rel, 400, "old query", "select 1")
        fixtures = self.write_fixture_files(
            {"list_queries.json": {"queries": [{"id": 400, "name": "old query", "updated_at": "2024-01-01T00:00:00Z"}]}}
        )

        summary = self.run_sync(fixtures)

        self.assertFalse((self.repo_root / rel).exists())
        moved = self.repo_root / "balancer" / "support" / "legacy" / "old_400.sql"
        self.assertTrue(moved.exists())
        self.assertEqual(summary["failed_count"], 0)
        self.assertIn(400, summary["manifest_category_updates"])

    def test_existing_managed_query_updates_in_place(self):
        self.write_manifest([{"id": 450, "category": "metrics/volume/protocol"}])
        self.write_classifier_files()
        rel = "balancer/metrics/volume/protocol/existing_450.sql"
        self.write_query_file(rel, 450, "existing", "select 1")
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 450, "name": "existing", "updated_at": "2099-01-01T00:00:00Z"}]},
                "query_450.json": {"query_id": 450, "name": "existing", "query_sql": "select 99"},
            }
        )

        summary = self.run_sync(fixtures)
        updated = (self.repo_root / rel).read_text(encoding="utf-8")

        self.assertIn("select 99", updated)
        self.assertIn(rel, summary["updated_files"])
        self.assertEqual(summary["updated_count"], 1)

    def test_owner_allowlist_filters_non_balancer_queries(self):
        self.write_manifest([])
        self.write_classifier_files()
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 500, "name": "Balancer query", "updated_at": "2026-04-10T05:00:00Z", "team_slug": "balancer"},
                        {"id": 501, "name": "Other query", "updated_at": "2026-04-10T05:00:00Z", "team_slug": "otherteam"},
                    ]
                },
                "query_500.json": {"query_id": 500, "name": "Balancer query", "query_sql": "select 500"},
            }
        )

        summary = self.run_sync(fixtures, owner_allowlist=["balancer"], require_owner_match=True)

        created_500 = list((self.repo_root / "balancer" / "unclassified").glob("*_500.sql"))
        created_501 = list((self.repo_root / "balancer" / "unclassified").glob("*_501.sql"))
        self.assertEqual(len(created_500), 1)
        self.assertEqual(len(created_501), 0)
        self.assertIn(501, summary["excluded_by_owner_ids"])

    def test_llm_fallback_only_when_ambiguous_and_capped(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.90,
                    "min_margin": 0.50,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [],
                "name_rules": [{"pattern": "balancer", "category": "metrics/volume/protocol", "weight": 1.0}],
                "sql_rules": [{"pattern": "select", "category": "metrics/volume/protocol", "weight": 1.0}],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 600, "name": "balancer one", "updated_at": "2026-04-10T05:00:00Z"},
                        {"id": 601, "name": "balancer two", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
                "query_600.json": {"query_id": 600, "name": "balancer one", "query_sql": "select 1"},
                "query_601.json": {"query_id": 601, "name": "balancer two", "query_sql": "select 2"},
            }
        )

        fake_llm = FakeLLM(category="metrics/volume/protocol", confidence=0.95)
        summary = self.run_sync(
            fixtures,
            llm_classifier=fake_llm,
            llm_enabled=True,
            llm_max_calls=1,
        )

        created_600 = list((self.repo_root / "balancer" / "metrics" / "volume" / "protocol").glob("*_600.sql"))
        created_601 = list((self.repo_root / "balancer" / "unclassified").glob("*_601.sql"))
        self.assertEqual(len(created_600), 1)
        self.assertEqual(len(created_601), 1)
        self.assertEqual(fake_llm.call_count, 1)
        self.assertEqual(summary["llm_calls_used"], 1)

    def test_llm_unlimited_when_max_calls_is_zero(self):
        self.write_manifest([])
        self.write_classifier_files(
            rules={
                "settings": {
                    "high_confidence_threshold": 0.90,
                    "min_margin": 0.50,
                    "min_signal_votes": 2,
                    "require_sql_or_dashboard": True,
                    "llm_confidence_threshold": 0.78,
                    "llm_top_k_candidates": 3,
                    "legacy_inactive_days": 180,
                },
                "signal_weights": {"name": 0.35, "sql": 0.45, "dashboard": 0.20},
                "special_name_rules": [],
                "name_rules": [{"pattern": "balancer", "category": "metrics/volume/protocol", "weight": 1.0}],
                "sql_rules": [{"pattern": "select", "category": "metrics/volume/protocol", "weight": 1.0}],
            }
        )
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 610, "name": "balancer one", "updated_at": "2026-04-10T05:00:00Z"},
                        {"id": 611, "name": "balancer two", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
                "query_610.json": {"query_id": 610, "name": "balancer one", "query_sql": "select 1"},
                "query_611.json": {"query_id": 611, "name": "balancer two", "query_sql": "select 2"},
            }
        )

        fake_llm = FakeLLM(category="metrics/volume/protocol", confidence=0.95)
        summary = self.run_sync(
            fixtures,
            llm_classifier=fake_llm,
            llm_enabled=True,
            llm_max_calls=0,
        )

        created_610 = list((self.repo_root / "balancer" / "metrics" / "volume" / "protocol").glob("*_610.sql"))
        created_611 = list((self.repo_root / "balancer" / "metrics" / "volume" / "protocol").glob("*_611.sql"))
        self.assertEqual(len(created_610), 1)
        self.assertEqual(len(created_611), 1)
        self.assertEqual(fake_llm.call_count, 2)
        self.assertEqual(summary["llm_calls_used"], 2)
        self.assertIsNone(summary["llm_max_calls"])

    def test_strict_endpoint_guard_blocks_disallowed_paths(self):
        client = sync.DuneHttpClient("dummy", strict_no_execution=True)
        with self.assertRaises(RuntimeError):
            client._ensure_allowed_endpoint("POST", "https://api.dune.com/api/v1/execution/abc/results")

    def test_summary_contains_usage_and_call_counts(self):
        self.write_manifest([])
        self.write_classifier_files()
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {"queries": [{"id": 700, "name": "new query", "updated_at": "2026-04-10T05:00:00Z"}]},
                "query_700.json": {"query_id": 700, "name": "new query", "query_sql": "select 700"},
                "usage.json": {
                    "billing_periods": [
                        {
                            "start_date": "2026-04-01",
                            "end_date": "2026-04-30",
                            "credits_used": 12.5,
                            "services": [{"name": "query_management", "credits_used": 1.25}],
                        }
                    ]
                },
            }
        )

        summary = self.run_sync(fixtures)
        self.assertIn("dune_call_counts", summary)
        self.assertIn("usage_before", summary)
        self.assertIn("usage_after", summary)
        self.assertIn("usage_delta_credits", summary)
        self.assertIn("usage_delta_services", summary)
        self.assertEqual(summary["strict_no_execution_mode"], False)


def json_to_yaml(payload: dict) -> str:
    # Keep test fixtures readable and deterministic.
    import yaml

    return yaml.safe_dump(payload, sort_keys=False)


if __name__ == "__main__":
    unittest.main()
