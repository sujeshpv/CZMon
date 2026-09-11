import json
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse

from collectors.scripts.check_underutilized_cluster import check_cluster_utilization
from collectors.scripts.task_monitor import get_tasks_from_pc
from coreapp.views import _load_stats_target_names, _normalize_stats_payload


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
            "endpoint_types": ["PE"],
            "datasets": [{"key": "value", "label": "Snapshots"}],
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
      connection.execute(
        """
        CREATE TABLE clusters (
          name TEXT,
          clusterExternalIPAddress TEXT,
          created_at TEXT
        )
        """
      )
      connection.execute(
        """
        INSERT INTO clusters (name, clusterExternalIPAddress, created_at)
        VALUES (?, ?, datetime('now'))
        """,
        ("PC Alpha", "10.1.1.20"),
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
    self.assertContains(response, 'data-range="12h"')
    self.assertNotContains(response, 'data-range="1h"')
    self.assertNotContains(response, 'data-range="6h"')

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

  def test_stats_api_rejects_unsupported_endpoint_type(self):
    response = self.client.get(
      reverse("stats_data_api"),
      {"stat": "snapshot_data", "endpoint_type": "PC"},
    )

    self.assertEqual(response.status_code, 400)
    self.assertIn("does not support PC", response.json()["error"])

  def test_stats_api_rejects_invalid_range(self):
    response = self.client.get(
      reverse("stats_data_api"),
      {"stat": "snapshot_data", "range": "forever"},
    )

    self.assertEqual(response.status_code, 400)

  def test_stats_api_accepts_twelve_hour_range(self):
    response = self.client.get(
      reverse("stats_data_api"),
      {"stat": "snapshot_data", "range": "12h"},
    )

    self.assertEqual(response.status_code, 200)

  def test_stats_target_names_are_loaded_from_collected_clusters(self):
    self.assertEqual(
      _load_stats_target_names(),
      {"10.1.1.20": "PC Alpha"},
    )

  def test_metric_payloads_are_normalized_into_chart_series(self):
    vm = _normalize_stats_payload(
      "vm_power_states",
      {
        "Cluster_name": "PE-1",
        "host-a": {
          "powered_on": 4,
          "powered_off": 2,
          "affinity_enabled_count": 1,
        },
      },
    )
    utilization = _normalize_stats_payload(
      "underutilized_cluster",
      {"cpu_usage_percent": 12.5, "memory_usage_percent": 30, "iops": 90},
    )
    pgw = _normalize_stats_payload("pgw_status", {"is_online": True})
    tasks = _normalize_stats_payload(
      "task_monitor",
      {
        "Pending": [{"pending-1": "Running task"}],
        "Failed": [{"failed-1": "Failure detail"}],
      },
    )

    self.assertEqual(
      vm["values"],
      {"powered_on": 4, "powered_off": 2, "affinity": 1},
    )
    self.assertEqual(
      utilization["values"],
      {"cpu": 12.5, "memory": 30.0, "iops": 90.0},
    )
    self.assertEqual(pgw["values"], {"status": 1})
    self.assertEqual(tasks["values"], {"pending": 1, "failed": 1})
    self.assertEqual(tasks["details"]["total"], 2)

  def test_ahv_normalization_keeps_all_partition_details(self):
    payload = {
      "cluster-a": [
        {
          "host-a": {
            "/": {"total": "100G", "available": "20G", "usage": "80%"},
            "/home": {"total": "50G", "available": "30G", "usage": "40%"},
          }
        }
      ]
    }

    normalized = _normalize_stats_payload("ahv_partition_usage", payload)

    self.assertEqual(normalized["values"], {"usage": 80.0})
    self.assertEqual(normalized["details"], payload)


class TaskMonitorCollectorTests(SimpleTestCase):
  @patch("collectors.scripts.task_monitor.requests.get")
  def test_task_collector_keeps_failed_and_overdue_active_tasks(self, mock_get):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
      "data": [
        {
          "extId": "done-1",
          "status": "SUCCEEDED",
          "operationDescription": "Completed task",
          "createdTime": "2026-08-17T05:00:00Z",
        },
        {
          "extId": "run-1",
          "status": "RUNNING",
          "operationDescription": "Running task",
          "createdTime": "2026-08-17T05:00:00Z",
        },
        {
          "extId": "queue-1",
          "status": "QUEUED",
          "operationDescription": "Queued task",
          "createdTime": "2026-08-17T05:00:00Z",
        },
        {
          "extId": "failed-1",
          "status": "FAILED",
          "operationDescription": "Failed task",
          "createdTime": "2026-08-17T05:00:00Z",
          "errorMessages": [{"message": "Failure detail"}],
        },
      ]
    }
    mock_get.return_value = response

    result = get_tasks_from_pc("10.1.1.1", "admin", "secret", 2, 0.25)

    self.assertEqual(len(result["Pending"]), 2)
    self.assertEqual(len(result["Failed"]), 1)
    self.assertEqual(result["Failed"][0], {"failed-1": "Failure detail"})


class UnderutilizedClusterCollectorTests(SimpleTestCase):
  @patch("collectors.scripts.check_underutilized_cluster.requests.get")
  def test_memory_uses_hypervisor_memory_ppm(self, mock_get):
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {
      "stats": {
        "hypervisor_cpu_usage_ppm": "402500",
        "hypervisor_memory_usage_ppm": "581604",
        "controller_num_iops": "1750",
      }
    }
    mock_get.return_value = response

    result = check_cluster_utilization("10.1.1.1", "admin", "secret")

    self.assertEqual(result["cpu_usage_percent"], 40.25)
    self.assertEqual(result["memory_usage_percent"], 58.16)
    self.assertEqual(result["iops"], 1750.0)
