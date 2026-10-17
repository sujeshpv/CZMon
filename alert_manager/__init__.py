"""CZMon Alert Management Framework.

This package provides a plugin-based alert system for monitoring metrics
collected by CZMon.

Main Components:
    - AlertManager: Orchestrates alert policy execution
    - AlertPlugin: Base class for creating alert plugins

Features:
    - Query-driven configuration via JSON
    - Dynamic template substitution (table names, params, etc.)
    - Automatic template processing for all plugins

Usage:
    from alert_manager.alert_manager import AlertManager

    manager = AlertManager()
    manager.load_policies()
    results = manager.execute_all_policies()
"""

from alert_manager.alert_manager import AlertManager
from alert_manager.base_plugin import AlertPlugin

__all__ = ["AlertManager", "AlertPlugin"]
