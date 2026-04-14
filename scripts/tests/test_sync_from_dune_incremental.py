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


class SyncFromDuneIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sync-from-dune-test-")
        self.repo_root = Path(self.tmp) / "repo"
        self.repo_root.mkdir(parents=True, exist_ok=True)
        (self.repo_root / "balancer").mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def write_manifest(self, entries):
        lines = ["query_ids:"]
        for entry in entries:
            if isinstance(entry, dict):
                lines.append(f"  - id: {entry['id']}")
                if entry.get("category"):
                    lines.append(f"    category: {entry['category']}")
            else:
                lines.append(f"  - {entry}")
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

    def run_sync(self, fixtures_dir: Path, dry_run: bool = False):
        # Make sure no network-based watermark lookup is attempted in tests.
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
                owner_allowlist=[],
                require_owner_match=False,
                workflow_file="sync_from_dune.yml",
                summary_path=summary_path,
            )
        finally:
            if old_repo is not None:
                os.environ["GITHUB_REPOSITORY"] = old_repo
            if old_token is not None:
                os.environ["GITHUB_TOKEN"] = old_token
            if old_run_id is not None:
                os.environ["GITHUB_RUN_ID"] = old_run_id

    def test_new_query_creates_file_and_manifest_entry(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2020-01-01T00:00:00Z"},
                        {"id": 200, "name": "Brand New", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
                "query_200.json": {"query_id": 200, "name": "Brand New", "query_sql": "select 2"},
            }
        )

        summary = self.run_sync(fixtures)

        created = list((self.repo_root / "balancer" / "unclassified").glob("*_200.sql"))
        self.assertEqual(len(created), 1)
        manifest_text = (self.repo_root / "queries.yml").read_text(encoding="utf-8")
        self.assertIn("id: 200", manifest_text)
        self.assertIn("category: unclassified", manifest_text)
        self.assertEqual(summary["new_count"], 1)

    def test_existing_query_unchanged_does_not_modify_file(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])
        rel = "balancer/metrics/volume/protocol/existing_100.sql"
        self.write_query_file(rel, 100, "Existing", "select 1")
        before = (self.repo_root / rel).read_text(encoding="utf-8")

        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2099-01-01T00:00:00Z"}
                    ]
                },
                "query_100.json": {"query_id": 100, "name": "Existing", "query_sql": "select 1"},
            }
        )

        summary = self.run_sync(fixtures)
        after = (self.repo_root / rel).read_text(encoding="utf-8")

        self.assertEqual(before, after)
        self.assertEqual(summary["changes_total"], 0)
        self.assertIn(100, summary["unchanged_ids"])

    def test_existing_query_changed_updates_file(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])
        rel = "balancer/metrics/volume/protocol/existing_100.sql"
        self.write_query_file(rel, 100, "Existing", "select 1")

        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2099-01-01T00:00:00Z"}
                    ]
                },
                "query_100.json": {"query_id": 100, "name": "Existing", "query_sql": "select 999"},
            }
        )

        summary = self.run_sync(fixtures)
        after = (self.repo_root / rel).read_text(encoding="utf-8")

        self.assertIn("select 999", after)
        self.assertEqual(summary["updated_count"], 1)
        self.assertIn(rel, summary["updated_files"])

    def test_pagination_fixtures_across_multiple_pages(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])

        fixtures = self.write_fixture_files(
            {
                "list_queries_page1.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2020-01-01T00:00:00Z"}
                    ]
                },
                "list_queries_page2.json": {
                    "queries": [
                        {"id": 201, "name": "Page Two Query", "updated_at": "2026-04-10T05:00:00Z"}
                    ]
                },
                "query_201.json": {
                    "query_id": 201,
                    "name": "Page Two Query",
                    "query_sql": "select 201",
                },
            }
        )

        summary = self.run_sync(fixtures)
        created = list((self.repo_root / "balancer" / "unclassified").glob("*_201.sql"))

        self.assertEqual(len(created), 1)
        self.assertIn(201, summary["new_ids_appended_to_manifest"])

    def test_missing_query_payload_is_reported_and_skipped(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])
        rel = "balancer/metrics/volume/protocol/existing_100.sql"
        self.write_query_file(rel, 100, "Existing", "select 1")

        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2099-01-01T00:00:00Z"}
                    ]
                }
            }
        )

        summary = self.run_sync(fixtures)

        self.assertEqual(summary["changes_total"], 0)
        self.assertEqual(summary["failed_count"], 1)
        self.assertIn(100, summary["failed_ids"])

    def test_dry_run_does_not_persist_changes(self):
        self.write_manifest([{"id": 100, "category": "metrics/volume/protocol"}])
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {"id": 100, "name": "Existing", "updated_at": "2020-01-01T00:00:00Z"},
                        {"id": 300, "name": "Dry Run Query", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
                "query_300.json": {
                    "query_id": 300,
                    "name": "Dry Run Query",
                    "query_sql": "select 300",
                },
            }
        )

        summary = self.run_sync(fixtures, dry_run=True)

        self.assertEqual(summary["changes_total"], 1)
        self.assertFalse((self.repo_root / "balancer" / "unclassified").exists())
        manifest_text = (self.repo_root / "queries.yml").read_text(encoding="utf-8")
        self.assertNotIn("id: 300", manifest_text)

    def test_owner_allowlist_filters_out_non_balancer_queries(self):
        self.write_manifest([])
        fixtures = self.write_fixture_files(
            {
                "list_queries.json": {
                    "queries": [
                        {
                            "id": 700,
                            "name": "Balancer query",
                            "updated_at": "2026-04-10T05:00:00Z",
                            "team_slug": "balancer",
                        },
                        {
                            "id": 701,
                            "name": "Other team query",
                            "updated_at": "2026-04-10T05:00:00Z",
                            "team_slug": "randomteam",
                        },
                    ]
                },
                "query_700.json": {"query_id": 700, "name": "Balancer query", "query_sql": "select 700"},
            }
        )

        client = sync.FixtureDuneClient(fixtures)
        summary = sync.sync_queries(
            repo_root=self.repo_root,
            client=client,
            lookback_hours=30,
            dry_run=False,
            default_new_category="unclassified",
            owner_allowlist=["balancer"],
            require_owner_match=True,
            workflow_file="sync_from_dune.yml",
            summary_path=Path(self.tmp) / "summary-filter.json",
        )

        created_700 = list((self.repo_root / "balancer" / "unclassified").glob("*_700.sql"))
        created_701 = list((self.repo_root / "balancer" / "unclassified").glob("*_701.sql"))
        self.assertEqual(len(created_700), 1)
        self.assertEqual(len(created_701), 0)
        self.assertIn(701, summary["excluded_by_owner_ids"])


if __name__ == "__main__":
    unittest.main()
