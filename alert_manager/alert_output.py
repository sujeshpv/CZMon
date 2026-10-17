"""Common library for alert JSON output generation and validation."""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
import logging

LOG = logging.getLogger(__name__)


# Required keys in alert JSON output
REQUIRED_KEYS = [
    "policy_name",
    "policy_status",
    "policy_description",
    "priority",
    "issue",
    "owner",
    "date_time",
    "cluster_details"
]


class AlertOutputValidator:
    """Validator for alert JSON output format."""
    
    @staticmethod
    def validate_json_output(json_data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Validate that JSON output contains all required keys.
        
        Args:
            json_data: JSON data to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        missing_keys = []
        
        for key in REQUIRED_KEYS:
            if key not in json_data:
                missing_keys.append(key)
        
        if missing_keys:
            error_msg = f"Missing required keys in JSON output: {', '.join(missing_keys)}"
            LOG.error(error_msg)
            return False, error_msg
        
        # Validate data types
        if not isinstance(json_data["policy_status"], bool):
            return False, "policy_status must be a boolean"
        
        if json_data["priority"] not in ["P0", "P1", "P2"]:
            return False, "priority must be one of: P0, P1, P2"
        
        if not isinstance(json_data["cluster_details"], list):
            return False, "cluster_details must be a list"
        
        LOG.info("JSON output validation passed")
        return True, None


class AlertOutputGenerator:
    """Generator for alert JSON output."""
    
    # Output directory for JSON files
    OUTPUT_DIR = "alert_manager/output"
    
    def __init__(self, policy_id: str, logger: Optional[logging.Logger] = None):
        """
        Initialize output generator.
        
        Args:
            policy_id: Policy identifier
            logger: Logger instance (optional)
        """
        self.policy_id = policy_id
        self.logger = logger or LOG
    
    def generate_json_output(
        self,
        policy_status: bool,
        policy_description: str,
        priority: str,
        issue: str,
        cluster_details: list,
        owner: str = ""
    ) -> Dict[str, Any]:
        """
        Generate JSON output in the standard format.
        
        Args:
            policy_status: True if passed, False if failed
            policy_description: Description of what the policy does
            priority: P0/P1/P2 based on severity
            issue: Description of what failed (empty string if passed)
            cluster_details: List of cluster information
            owner: Owner/assignee (optional, defaults to empty string)
            
        Returns:
            Dict in the standard JSON format
        """
        json_output = {
            "policy_name": self.policy_id,
            "policy_status": policy_status,
            "policy_description": policy_description,
            "priority": priority,
            "issue": issue,
            "owner": owner,
            "date_time": datetime.now().isoformat(),
            "cluster_details": cluster_details
        }
        
        # Validate before returning
        is_valid, error_msg = AlertOutputValidator.validate_json_output(json_output)
        if not is_valid:
            self.logger.error(f"Generated JSON output failed validation: {error_msg}")
            raise ValueError(f"Invalid JSON output: {error_msg}")
        
        return json_output
    
    def write_json_output(self, json_data: Dict[str, Any]) -> str:
        """
        Write JSON data to output file.
        
        Args:
            json_data: JSON data to write
            
        Returns:
            Path to the written file
        """
        # Validate before writing
        is_valid, error_msg = AlertOutputValidator.validate_json_output(json_data)
        if not is_valid:
            raise ValueError(f"Cannot write invalid JSON output: {error_msg}")
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.policy_id}_{timestamp}.json"
        filepath = os.path.join(self.OUTPUT_DIR, filename)
        
        # Write JSON file
        with open(filepath, 'w') as f:
            json.dump(json_data, f, indent=2)
        
        self.logger.info(f"Alert output written to: {filepath}")
        return filepath


def create_error_output(
    policy_id: str,
    policy_description: str,
    error_message: str,
    logger: Optional[logging.Logger] = None
) -> tuple[Dict[str, Any], str]:
    """
    Create and write JSON output for error cases.
    
    Args:
        policy_id: Policy identifier
        policy_description: Policy description
        error_message: Error message
        logger: Logger instance (optional)
        
    Returns:
        Tuple of (json_data, output_file_path)
    """
    generator = AlertOutputGenerator(policy_id, logger)
    
    json_output = generator.generate_json_output(
        policy_status=False,
        policy_description=policy_description,
        priority="P1",
        issue=error_message,
        cluster_details=[]
    )
    
    output_file = generator.write_json_output(json_output)
    return json_output, output_file
