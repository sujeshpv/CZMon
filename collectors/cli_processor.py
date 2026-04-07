from common.connection.sqliteworker import Sqlite3Worker
from common.connection.ssh_connect import Ssh
from common.logger.logger import EntryExit, setup_logger
from collectors.api_processor import ApiProcessor
from common.exceptions.exceptions import *
from library.const import NUTANIX
from library.urls import CLUSTER
import os

LOGGER = setup_logger(__name__)


class CliProcessor:
  """
  CliProcessor is responsible for executing CLI commands on
  remote systems (PC/PE) via SSH and storing results in DB.
  """
  def __init__(self):
    """
    Initialize DB worker for storing CLI results.
    """
    try:
      self.db_worker = Sqlite3Worker("metrics.db")
      self.api_processor = ApiProcessor()
    except Exception as err:
      error = CZMonError(
        "Failed initializing CliProcessor",
        cause=err
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def process_data(self, config):
    """
    Execute CLI commands for given configuration and persist results.

    Parameters
    ----------
    config : dict
      Configuration containing table name, endpoint type,
      and list of commands to execute.
    """
    try:
      for table_name, entity_data in config.items():
        endpoint_type = entity_data.get("endpoint_type")
        commands = entity_data.get("command", [])
        system_config = {
          "ips": os.environ.get(f"{endpoint_type}_IPS", "").split(",")
        }
        for ip in system_config.get("ips"):
          try:
            ssh_obj = Ssh(ip, NUTANIX)
            for command in commands:
              try:
                values = {}
                full_command = (f"bash -lc 'for i in $(svmips); "
                                f"do echo \"================== $i "
                                f"=================\"; ssh $i "
                                f"{command}; done'")
                output = ssh_obj.execute(full_command)
                LOGGER.info(
                  "Output for command '%s' on %s: %s",
                  full_command, ip, output
                )
                values["command"] = command
                values["output"] = output
                values["ip"] = ip
                values["cluster_name"] = (
                  f"$({CLUSTER}#name#clusterExternalIPAddress={ip})"
                )
                values = self.api_processor.fetch_dynamic_values(values)
                self.db_worker.ensure_schema(table_name, values)
                self.db_worker.insert_row(table_name, values)
              except Exception as cmd_err:
                error = CZMonError(
                  "Command execution failed",
                  cause=cmd_err,
                  context={
                    "ip": ip,
                    "command": command,
                    "table": table_name
                  }
                )
                LOGGER.error(error)
                continue
          except Exception as ssh_err:
            error = CZMonError(
              "SSH connection failed",
              cause=ssh_err,
              context={
                "ip": ip,
                "endpoint_type": endpoint_type
              }
            )
            LOGGER.error(error)
            continue
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "CLI processing failed",
        cause=err
      )
      LOGGER.error(error)
      raise error
