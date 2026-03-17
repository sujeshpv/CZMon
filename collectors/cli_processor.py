from common.connection.sqliteworker import Sqlite3Worker
from common.connection.ssh_connect import Ssh
from common.logger.logger import EntryExit, setup_logger
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
    except Exception as err:
      LOGGER.exception("Failed initializing CliProcessor: %s", err)
      raise

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
          "ips": os.environ.get(f"{endpoint_type}_IPS", "").split(","),
          "cvm_username": os.environ.get(f"{endpoint_type}_CVM_USERNAME"),
          "cvm_password": os.environ.get(f"{endpoint_type}_CVM_PASSWORD"),
        }

        username = system_config.get("cvm_username")
        password = system_config.get("cvm_password")

        if not username or not password:
          raise ValueError(f"Missing credentials for {endpoint_type}")

        for ip in system_config.get("ips"):
          try:
            ssh_obj = Ssh(ip, username, password)

            for command in commands:
              try:
                values = {}

                output = ssh_obj.execute(command)

                LOGGER.info(
                  f"Output for command '{command}' on {ip}: {output}"
                )

                values["command"] = command
                values["output"] = output
                values["ip"] = ip

                self.db_worker.ensure_schema(table_name, values)
                self.db_worker.insert_row(table_name, values)

              except Exception as cmd_err:
                LOGGER.exception(
                  "Command execution failed for %s on %s: %s",
                  command, ip, cmd_err
                )

          except Exception as ssh_err:
            LOGGER.exception(
              "SSH connection failed for %s: %s",
              ip, ssh_err
            )

    except Exception as err:
      LOGGER.exception("CLI processing failed: %s", err)
      raise
