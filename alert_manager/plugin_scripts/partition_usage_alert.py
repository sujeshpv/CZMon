"""Alert plugin for AHV partition usage monitoring."""

import json
from alert_manager.base_plugin import AlertPlugin
from alert_manager.alert_output import AlertOutputGenerator
from typing import Dict, Any


class PartitionUsageAlert(AlertPlugin):
    """Monitor AHV partition usage - IPs loaded from endpoints.json automatically."""
    
    # Hardcoded thresholds
    WARNING_THRESHOLD = 80.0
    CRITICAL_THRESHOLD = 90.0
    
    def check_partition_usage(self, usage_data: list) -> Dict[str, Any]:
        """
        Check partition usage against thresholds.
        
        Args:
            usage_data: List of query results with status_data
            
        Returns:
            Dict with warnings and criticals lists
        """
        warnings = []
        criticals = []
            
        # Process each row from the query
        for row in usage_data:
            status_data_raw = row.get("status_data", {})
            
            # Parse JSON if it's a string
            if isinstance(status_data_raw, str):
                try:
                    status_data = json.loads(status_data_raw)
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse status_data JSON: {status_data_raw[:100]}")
                    continue
            else:
                status_data = status_data_raw
            
            for cluster_id, nodes in status_data.items():
                if not isinstance(nodes, list):
                    continue
                
                for node_data in nodes:
                    for node_name, node_info in node_data.items():
                        partitions = node_info.get("partitions", {})
                        
                        # Check each partition
                        for partition_path, partition_info in partitions.items():
                            usage_str = partition_info.get("usage")
                            
                            # Parse usage (e.g., "85%" -> 85.0)
                            if isinstance(usage_str, str) and "%" in usage_str:
                                usage = float(usage_str.rstrip("%"))
                            else:
                                usage = float(usage_str)
                            
                            # Check thresholds
                            if usage >= self.CRITICAL_THRESHOLD:
                                criticals.append({
                                    "node": node_name,
                                    "partition": partition_path,
                                    "usage": usage,
                                    "total": partition_info.get("total"),
                                    "available": partition_info.get("available")
                                })
                            elif usage >= self.WARNING_THRESHOLD:
                                warnings.append({
                                    "node": node_name,
                                    "partition": partition_path,
                                    "usage": usage,
                                    "total": partition_info.get("total"),
                                    "available": partition_info.get("available")
                                })
        
        return {
            "warnings": warnings,
            "criticals": criticals
        }
    
    def _build_issue_description(self, all_warnings: list, all_criticals: list) -> str:
        """
        Build issue description string from warnings and criticals.
        
        Args:
            all_warnings: List of warning violations
            all_criticals: List of critical violations
            
        Returns:
            str: Formatted issue description string, empty if no issues
        """
        if all_criticals:
            issue_parts = []
            for item in all_criticals:
                issue_parts.append(
                    f"Node {item['node']}: Partition {item['partition']} at {item['usage']}% "
                    f"(threshold: {self.CRITICAL_THRESHOLD}%)"
                )
            return "CRITICAL: " + "; ".join(issue_parts)
        elif all_warnings:
            issue_parts = []
            for item in all_warnings:
                issue_parts.append(
                    f"Node {item['node']}: Partition {item['partition']} at {item['usage']}% "
                    f"(threshold: {self.WARNING_THRESHOLD}%)"
                )
            return "WARNING: " + "; ".join(issue_parts)
        else:
            return ""
    
    def _extract_cluster_details(self, latest_usage_data: list) -> list:
        """
        Extract cluster details from usage data.
        
        Args:
            latest_usage_data: List of database rows with status_data
            
        Returns:
            list: List of cluster detail dictionaries with cluster_id and nodes_checked
        """
        cluster_details = []
        for row in latest_usage_data:
            status_data_raw = row.get("status_data", {})
            
            # Parse JSON if it's a string
            if isinstance(status_data_raw, str):
                try:
                    status_data = json.loads(status_data_raw)
                except json.JSONDecodeError:
                    self.logger.warning(f"Failed to parse status_data JSON: {status_data_raw[:100]}")
                    continue
            else:
                status_data = status_data_raw
            
            for cluster_id, nodes in status_data.items():
                cluster_info = {
                    "cluster_id": cluster_id,
                    "nodes_checked": len(nodes) if isinstance(nodes, list) else 0
                }
                cluster_details.append(cluster_info)
        return cluster_details
    
    def _generate_and_write_output(self, status: str, all_warnings: list, 
                                   all_criticals: list, latest_usage_data: list) -> tuple[Dict[str, Any], str]:
        """Generate and write JSON output using common library.
        
        Returns:
            Tuple of (json_output, output_file_path)
        """
        # Initialize output generator
        output_gen = AlertOutputGenerator(self.policy_id, self.logger)
        
        # Determine priority and policy_status
        if status == "CRITICAL":
            priority = "P0"
            policy_status = False
        elif status == "WARNING":
            priority = "P1"
            policy_status = False
        else:  # OK
            priority = "P2"
            policy_status = True
        
        # Build components
        issue = self._build_issue_description(all_warnings, all_criticals)
        cluster_details = self._extract_cluster_details(latest_usage_data)
        policy_description = (
            f"Monitor AHV partition usage across all nodes. "
            f"Alert if any partition exceeds {self.WARNING_THRESHOLD}% (warning) "
            f"or {self.CRITICAL_THRESHOLD}% (critical)."
        )
        
        # Generate JSON output (with validation)
        json_output = output_gen.generate_json_output(
            policy_status=policy_status,
            policy_description=policy_description,
            priority=priority,
            issue=issue,
            cluster_details=cluster_details
        )
        
        # Write to file (with validation)
        output_file = output_gen.write_json_output(json_output)
        return json_output, output_file
    
    def _fetch_partition_usage_data(self) -> list:
        """
        Fetch partition usage data from database.
        
        Returns:
            list: Query results with status_data
        """
        table_name = self.config.get("table_name", "check_ahv_home_usage")
        query = f"SELECT status_data FROM {table_name} WHERE created_at >= datetime('now', '-17 days');"
        
        results = self.db_worker.execute(query, return_as_dict=True)
        return results if results else []
    
    def run_partition_usage_alert(self) -> Dict[str, Any]:
        """
        Run partition usage alert check with all logic.
        
        Fetches data, checks thresholds, generates JSON output,
        and returns formatted alert result.
        
        Returns:
            dict: Formatted alert result with status, message, triggered flag, and details
        """
        # Fetch partition usage data
        latest_usage_data = self._fetch_partition_usage_data()
        
        if not latest_usage_data:
            # Generate JSON output for error case using common library
            output_gen = AlertOutputGenerator(self.policy_id, self.logger)
            policy_description = (
                f"Monitor AHV partition usage across all nodes. "
                f"Alert if any partition exceeds {self.WARNING_THRESHOLD}% (warning) "
                f"or {self.CRITICAL_THRESHOLD}% (critical)."
            )
            
            json_output = output_gen.generate_json_output(
                policy_status=False,
                policy_description=policy_description,
                priority="P1",
                issue="No partition usage data found in database",
                cluster_details=[]
            )
            output_file = output_gen.write_json_output(json_output)
            
            return self.format_result(
                status="ERROR",
                message="No partition usage data found",
                triggered=False,
                details={"output_file": output_file, "json_output": json_output}
            )
        
        # Run individual checks
        partition_check = self.check_partition_usage(latest_usage_data)
        
        # Consolidate results
        all_warnings = partition_check["warnings"]
        all_criticals = partition_check["criticals"]
        
        # Determine overall status
        if all_criticals:
            status = "CRITICAL"
            triggered = True
            message = f"{len(all_criticals)} partition(s) in CRITICAL state (>={self.CRITICAL_THRESHOLD}%)"
        elif all_warnings:
            status = "WARNING"
            triggered = True
            message = f"{len(all_warnings)} partition(s) in WARNING state (>={self.WARNING_THRESHOLD}%)"
        else:
            status = "OK"
            triggered = False
            message = "All partition usage within normal thresholds"
        
        # Generate and write JSON output using common library (with validation)
        json_output, output_file = self._generate_and_write_output(status, all_warnings, all_criticals, latest_usage_data)
        
        # Build details for internal use
        details = {
            "critical_count": len(all_criticals),
            "warning_count": len(all_warnings),
            "thresholds": {
                "warning": self.WARNING_THRESHOLD,
                "critical": self.CRITICAL_THRESHOLD
            },
            "output_file": output_file,
            "json_output": json_output
        }
        
        if all_criticals:
            details["critical_partitions"] = all_criticals
        if all_warnings:
            details["warning_partitions"] = all_warnings
        
        return self.format_result(status, message, triggered, details)
    
    def execute_alert(self) -> Dict[str, Any]:
        """
        Execute all alerts - orchestrator function that only calls other functions.
        
        This function contains no logic, only calls to individual alert functions.
        Multiple alerts can be added and consolidated here.
        
        Returns:
            dict: Formatted alert result with status, message, triggered flag, and details
        """
        try:
            # Call individual alert functions
            return self.run_partition_usage_alert()
        except Exception as e:
            self.logger.error(f"Alert execution failed: {e}", exc_info=True)
            return self.format_result(
                status="ERROR",
                message=f"Alert execution failed: {str(e)}",
                triggered=False
            )
