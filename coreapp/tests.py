import json
import sqlite3
import tempfile
from pathlib import Path

from django.test import TestCase, override_settings
from django.urls import reverse


class StatsFrameworkTests(TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.temp_dir.name)
        config_dir = self.base_dir / "static" / "configurations"
        config_dir.mkdir(parents=True)
        (config_dir / "stats_registry.json").write_text(
            json.dumps(
                {
                    "snapshot_data": {
                        "name": "Snapshot Data",
                        "table_name": "snapshot_stats",
                        "summary": "Snapshot usage and age.",
                        "page": "stats_pages/time_series.html",
                        "parser": "generic",
                        "y_axis": "Snapshots",
                        "is_percentage": False,
                    }
                }
            ),
            encoding="utf-8",
        )

        with sqlite3.connect(self.base_dir / "metrics.db") as connection:
            connection.execute(
                """
                CREATE TABLE snapshot_stats (
                    id INTEGER PRIMARY KEY,
                    ip_address TEXT,
                    status_data TEXT,
                    created_at TEXT
                )
                """
            )
            connection.execute(
                """
                INSERT INTO snapshot_stats (ip_address, status_data, created_at)
                VALUES (?, ?, datetime('now'))
                """,
                ("10.1.1.10", json.dumps({"snapshots": 4})),
            )

        self.settings_override = override_settings(BASE_DIR=self.base_dir)
        self.settings_override.enable()

    def tearDown(self):
        self.settings_override.disable()
        self.temp_dir.cleanup()

    def test_selector_loads_configured_page_after_selection(self):
        response = self.client.get(reverse("stats"), {"stat": "snapshot_data"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Snapshot Data")
        self.assertContains(response, "Snapshot usage and age.")
        self.assertContains(response, 'id="statsChart"')

    def test_stats_api_resolves_table_from_registry(self):
        response = self.client.get(
            reverse("stats_data_api"),
            {"stat": "snapshot_data", "range": "90d"},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["name"], "Snapshot Data")
        self.assertEqual(len(body["series"]), 1)
        self.assertEqual(body["series"][0]["ip"], "10.1.1.10")

    def test_stats_api_rejects_unknown_stat(self):
        response = self.client.get(
            reverse("stats_data_api"),
            {"stat": "not_configured"},
        )

        self.assertEqual(response.status_code, 400)
