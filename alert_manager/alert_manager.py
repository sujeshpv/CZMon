"""Alert Manager orchestrates alert policy execution.

This module loads alert policies from configuration files, dynamically imports
plugin scripts, and executes alert checks.
"""

import argparse
import importlib.util
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

# Add parent directory to path for local imports
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

# Local imports (after sys.path modification)
from common.connection.sqliteworker import Sqlite3Worker
from alert_manager.base_plugin import AlertPlugin


class AlertManager:
    """Manages alert policies and plugin execution.

    Attributes:
        config_dir (str): Directory containing alert policy configs.
        db_helper (AlertDbHelper): Database helper
            instance.
        policies (dict): Loaded alert policies.
        logger: Logger instance.
    """

    def __init__(
            self, config_dir: Optional[str] = None,
            db_path: Optional[str] = None
    ):
        """Initialize Alert Manager.

        Args:
            config_dir (str, optional): Path to configs
                directory. Auto-resolves if None.
            db_path (str, optional): Path to metrics.db. Auto-resolves if None.
        """
        self.logger = logging.getLogger(__name__)

        # Auto-resolve config directory
        if not config_dir:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_dir = os.path.join(base_dir, "configs")

        # Auto-resolve database path
        if not db_path:
            base_dir = os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
            db_path = os.path.join(base_dir, "metrics.db")

        self.config_dir = config_dir
        self.db_worker = Sqlite3Worker(db_path)
        self.policies = {}
        self.endpoints = {}
        self.logger.info(
            f"AlertManager initialized with config directory: "
            f"{config_dir}"
        )

        # Load endpoints.json for dynamic IP resolution
        self._load_endpoints()

    def _load_endpoints(self):
        """Load endpoints.json for dynamic IP/cluster reference.

        This allows policies to reference clusters/IPs from endpoints.json
        instead of hardcoding them in alert configs.
        """
        try:
            # Look for endpoints.json in standard location
            base_dir = os.path.dirname(
                os.path.dirname(os.path.abspath(__file__))
            )
            endpoints_path = os.path.join(
                base_dir, "static", "configurations",
                "endpoints.json"
            )

            if os.path.exists(endpoints_path):
                with open(endpoints_path, "r") as f:
                    self.endpoints = json.load(f)
                self.logger.info(f"Loaded endpoints from {endpoints_path}")

                # Flatten endpoints for easy access
                self.all_endpoints = []
                for zone, zone_endpoints in self.endpoints.items():
                    if isinstance(zone_endpoints, list):
                        for endpoint in zone_endpoints:
                            endpoint_copy = endpoint.copy()
                            endpoint_copy["zone"] = zone
                            self.all_endpoints.append(endpoint_copy)

                self.logger.info(
                    f"Found {len(self.all_endpoints)} total "
                    f"endpoints"
                )
            else:
                self.logger.warning(
                    f"endpoints.json not found at "
                    f"{endpoints_path}"
                )
                self.endpoints = {}
                self.all_endpoints = []
        except Exception as e:
            self.logger.error(f"Failed to load endpoints.json: {e}")
            self.endpoints = {}
            self.all_endpoints = []

    def load_policies(self, policy_file: str = "alert_policy_id.json") -> None:
        """Load alert policies from configuration file.

        Args:
            policy_file (str): Name of the policy configuration file.

        Raises:
            FileNotFoundError: If config file doesn't exist.
            ValueError: If config JSON is invalid.
        """
        config_path = os.path.join(self.config_dir, policy_file)

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Policy config not found: {config_path}")

        try:
            with open(config_path, "r") as f:
                self.policies = json.load(f)

            self.logger.info(
                f"Loaded {len(self.policies)} alert "
                f"policy/policies from {policy_file}"
            )

            # Validate policy structure
            for policy_id, policy in self.policies.items():
                self._validate_policy(policy_id, policy)

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {config_path}: {e}")

    def _validate_policy(self, policy_id: str, policy: Dict[str, Any]) -> None:
        """Validate alert policy structure.

        Args:
            policy_id (str): Policy identifier.
            policy (dict): Policy configuration.

        Raises:
            ValueError: If policy structure is invalid.
        """
        required_fields = ["policy_id", "plugin_path"]
        missing = [f for f in required_fields if f not in policy]

        if missing:
            raise ValueError(
                f"Policy {policy_id} missing required fields: "
                f"{missing}"
            )

        if policy["policy_id"] != policy_id:
            self.logger.warning(
                f"Policy ID mismatch: key={policy_id}, "
                f"value={policy['policy_id']}"
            )

    def _load_plugin(
            self, plugin_path: str, policy_id: str,
            config: Dict[str, Any]
    ) -> AlertPlugin:
        """Dynamically load and instantiate a plugin.

        Args:
            plugin_path (str): Relative path to plugin script.
            policy_id (str): Policy identifier.
            config (dict): Complete policy configuration.

        Returns:
            AlertPlugin: Instantiated plugin object.

        Raises:
            ImportError: If plugin cannot be loaded.
            TypeError: If plugin doesn't inherit from AlertPlugin.
        """
        # Resolve absolute path
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(base_dir, plugin_path)

        if not os.path.exists(full_path):
            raise ImportError(f"Plugin script not found: {full_path}")

        # Dynamically import the module
        module_name = Path(plugin_path).stem
        spec = importlib.util.spec_from_file_location(module_name, full_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # Find the AlertPlugin subclass
        plugin_class = None
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) and
                    issubclass(attr, AlertPlugin) and
                    attr is not AlertPlugin):
                plugin_class = attr
                break

        if not plugin_class:
            raise TypeError(f"No AlertPlugin subclass found in {plugin_path}")

        # Instantiate plugin with new structure
        # (policy_id, config, db_worker, endpoints)
        plugin = plugin_class(
            policy_id, config, self.db_worker,
            self.endpoints, self.all_endpoints
        )
        self.logger.info(
            f"Loaded plugin: {plugin_class.__name__} "
            f"for policy {policy_id}"
        )

        return plugin

    def execute_policy(self, policy_id: str) -> Dict[str, Any]:
        """Execute a single alert policy.

        Args:
            policy_id (str): Policy identifier to execute.

        Returns:
            dict: Alert check result.

        Raises:
            KeyError: If policy_id doesn't exist.
        """
        if policy_id not in self.policies:
            raise KeyError(f"Policy not found: {policy_id}")

        policy = self.policies[policy_id]

        # Check if policy is enabled
        if not policy.get("enabled", True):
            self.logger.info(f"Policy {policy_id} is disabled, skipping")
            return {
                "policy_id": policy_id,
                "status": "DISABLED",
                "message": "Policy is disabled",
                "triggered": False
            }

        self.logger.info(
            f"Executing policy {policy_id}: "
            f"{policy.get('description', 'No description')}"
        )

        try:
            # Load plugin - pass full config
            plugin = self._load_plugin(
                plugin_path=policy["plugin_path"],
                policy_id=policy["policy_id"],
                config=policy
            )

            # Execute alert
            result = plugin.execute_alert()

            self.logger.info(
                f"Policy {policy_id} completed: "
                f"{result['status']}"
            )
            return result

        except Exception as e:
            self.logger.error(f"Policy {policy_id} failed: {e}", exc_info=True)
            return {
                "policy_id": policy_id,
                "status": "ERROR",
                "message": f"Plugin execution failed: {str(e)}",
                "triggered": False
            }

    def execute_all_policies(
            self, include_disabled: bool = False
    ) -> List[Dict[str, Any]]:
        """Execute all loaded alert policies.

        Args:
            include_disabled (bool): Include disabled policies. Default: False.

        Returns:
            list: List of alert check results.
        """
        results = []

        for policy_id, policy in self.policies.items():
            # Skip disabled policies unless explicitly requested
            if not include_disabled and not policy.get("enabled", True):
                self.logger.debug(f"Skipping disabled policy: {policy_id}")
                continue

            result = self.execute_policy(policy_id)
            results.append(result)

        self.logger.info(f"Executed {len(results)} alert policies")
        return results

    def execute_policies_by_table(
            self, table_name: str
    ) -> List[Dict[str, Any]]:
        """Execute all policies for a specific table.

        Args:
            table_name (str): Database table name.

        Returns:
            list: List of alert check results.
        """
        matching_policies = [
            pid for pid, policy in self.policies.items()
            if policy.get("table_name") == table_name
        ]

        results = []
        for policy_id in matching_policies:
            result = self.execute_policy(policy_id)
            results.append(result)

        return results

    def close(self):
        """
        Close database connections and cleanup resources.

        Should be called when AlertManager is no longer needed to properly
        close database connections.
        """
        if self.db_worker:
            self.db_worker.close()
        self.logger.info("AlertManager closed")


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # Create logger for main execution
    main_logger = logging.getLogger("alert_manager.main")

    parser = argparse.ArgumentParser(
        description="CZMon Alert Manager - Generic Execution"
    )
    parser.add_argument(
        "configs", nargs="*",
        help="Config file(s) - single or multiple"
    )
    parser.add_argument("--db", help="Database path")
    args = parser.parse_args()

    # Default if no configs provided
    config_files = args.configs if args.configs else ["alert_policy_id.json"]

    main_logger.info("=" * 70)
    main_logger.info(f"EXECUTING {len(config_files)} CONFIG FILE(S)")
    main_logger.info("=" * 70)

    all_results = []
    for config_file in config_files:
        main_logger.info(f"Processing: {config_file}")

        # Resolve paths
        config_dir = (
            os.path.dirname(os.path.abspath(config_file))
            if "/" in config_file else None
        )
        policy_file = os.path.basename(config_file)

        # Execute
        manager = AlertManager(config_dir=config_dir, db_path=args.db)
        manager.load_policies(policy_file=policy_file)
        results = manager.execute_all_policies()
        all_results.extend(results)

        # Log results
        for result in results:
            icon = "⚠️" if result.get('triggered') else "✓"
            main_logger.info(
                f"{icon} [{result['status']}] "
                f"{result['policy_id']}: {result['message']}"
            )

        manager.close()

    # Summary
    triggered = [r for r in all_results if r.get('triggered')]
    main_logger.info("=" * 70)
    main_logger.info(
        f"SUMMARY: {len(all_results)} total, "
        f"{len(triggered)} triggered"
    )
    main_logger.info("=" * 70)
