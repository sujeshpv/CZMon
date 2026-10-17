"""Base class for all alert plugins."""

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List


class AlertPlugin(ABC):
    """Base class for all alert plugins.
    
    All plugins must inherit from this class and implement the execute_alert() method.
    """
    
    def __init__(self, policy_id: str, config: Dict[str, Any], db_worker, 
                 endpoints: Dict[str, Any] = None, all_endpoints: List[Dict[str, Any]] = None):
        """Initialize the alert plugin.
        
        Args:
            policy_id: Policy identifier.
            config: Policy configuration.
            db_worker: Database worker instance.
            endpoints: Endpoints from endpoints.json.
            all_endpoints: Flattened list of all endpoints.
        """
        self.policy_id = policy_id
        self.config = config
        self.db_worker = db_worker
        self.endpoints = endpoints
        self.all_endpoints = all_endpoints
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        
        # Quick access to common config sections (optional)
        self.queries = config.get("queries", [])
        self.thresholds = config.get("thresholds", {})
        
        # Process templates automatically if queries exist
        if self.queries:
            self._process_query_placeholders()
    
    def _process_query_placeholders(self):
        """Process all templates in queries automatically.
        
        Replaces:
        - <table_name>, <time_range>, etc. from config
        - <ip_address> with actual IPs from endpoints.json based on endpoint_type
        """
        for query_config in self.queries:
            sql = query_config.get("sql", "")
            endpoint_type = query_config.get("endpoint_type")
            
            # Get IPs from endpoints.json based on endpoint_type
            # endpoint_type can be: "PE", ["PE", "PC"], or null (all)
            if "<ip_address>" in sql:
                if endpoint_type:
                    # Convert to list if string, keep as list if already list
                    types = [endpoint_type] if isinstance(endpoint_type, str) else endpoint_type
                    ips = [e["ip"] for e in self.all_endpoints if e.get("type") in types]
                else:
                    # No filter - get all IPs
                    ips = [e["ip"] for e in self.all_endpoints]
                query_config["_ips"] = ips  # Store IPs for execution
            
            # Replace all placeholders (supports nested keys with dot notation)
            # Example: <table_name>, <metadata.cluster_name>, <thresholds.warning>
            def replace_recursive(text, data, prefix=""):
                for key, value in data.items():
                    full_key = f"{prefix}.{key}" if prefix else key
                    if isinstance(value, dict):
                        text = replace_recursive(text, value, full_key)
                    else:
                        text = text.replace(f"<{full_key}>", str(value))
                return text
            
            query_config["_processed_sql"] = replace_recursive(sql, self.config)
    
    def execute_queries(self, return_as_dict: bool = False) -> Dict[str, List]:
        """Execute all queries for all IPs (optional - plugins can query directly).
        
        This method is optional. Plugins can choose to:
        - Use this method if they define queries in config
        - Query the database directly using self.db_worker.execute()
        
        Args:
            return_as_dict: If True, returns rows as dicts.
        
        Returns:
            dict: Query name -> list of results for all IPs.
        """
        results = {}
        for query_config in self.queries:
            name = query_config.get("name")
            sql = query_config.get("_processed_sql", query_config.get("sql"))
            ips = query_config.get("_ips", [])
            
            query_results = []
            if ips and "<ip_address>" in sql:
                # Execute query for each IP
                for ip in ips:
                    query_sql = sql.replace("<ip_address>", f"'{ip}'")
                    result = self.db_worker.execute_queries(query_sql, return_as_dict=return_as_dict)
                    if result:
                        # Add IP to each result row
                        if return_as_dict:
                            for row in result:
                                row["_ip"] = ip
                        query_results.extend(result)
            else:
                # Execute query as-is
                result = self.db_worker.execute(sql, return_as_dict=return_as_dict)
                if result:
                    query_results.extend(result)
            
            # Auto-parse JSON columns (like status_data)
            if return_as_dict and query_results:
                for row in query_results:
                    for key, value in row.items():
                        if isinstance(value, str) and value.strip().startswith('{'):
                            try:
                                row[key] = json.loads(value)
                            except (json.JSONDecodeError, ValueError):
                                pass  # Keep as string if not valid JSON
            
            results[name] = query_results
        
        return results
    
    @abstractmethod
    def execute_alert(self) -> Dict[str, Any]:
        """Execute the alert check logic.
        
        Returns:
            dict: Alert result with keys:
                - policy_id: Policy identifier
                - status: Alert status (OK, WARNING, CRITICAL, ERROR)
                - message: Human-readable message
                - triggered: Boolean indicating if alert triggered
                - details: Optional additional details
        """
        pass
    
    def format_result(self, status: str, message: str, triggered: bool, 
                      details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Format alert result in standard structure.
        
        Args:
            status: Alert status (OK, WARNING, CRITICAL, ERROR).
            message: Human-readable message.
            triggered: Whether alert was triggered.
            details: Optional additional details.
            
        Returns:
            dict: Formatted alert result.
        """
        return {
            "policy_id": self.policy_id,
            "status": status,
            "message": message,
            "triggered": triggered,
            "details": details or {}
        }
