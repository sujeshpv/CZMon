from collectors.metrics_processor import MetricsProcessor
from collectors.cli_processor import CliProcessor
from common.logger.logger import EntryExit, setup_logger
from library.const import *
import os
import json
import argparse
import logging
import threading
import time

LOGGER = setup_logger(__name__)


class Runner:
  """
  Runner is the entry point for the Metrics Collection Framework.

  It is responsible for:
  - Parsing command line arguments
  - Loading configuration files
  - Setting environment variables for PC and PE systems
  - Launching metric collection threads
  - Coordinating execution of the MetricsProcessor
  """

  def __init__(self):
    """
    Initialize the Runner with configuration paths and
    a MetricsProcessor instance.
    """
    try:
      self.args = None
      self.config = None
      self.pc_ips = []
      self.pe_ips = []

      self.endpoint_config = os.path.join(
          STATIC, CONFIGURATIONS, ENDPOINTS_JSON
      )

      self.metrics_config = os.path.join(STATIC, CONFIGURE, API_METRICS_CATALOG_JSON)
      self.cli_config = os.path.join(STATIC, CONFIGURE, CLI_METRICS_CATALOG_JSON)

      self.metric_processor = MetricsProcessor()
      self.cli_processor = CliProcessor()

    except Exception as err:
      LOGGER.exception("Failed initializing Runner: %s", err)
      raise

  @EntryExit
  def parse_args(self):
    """
    Parse command line arguments for the metrics framework.

    Supported arguments:
    --pc-ip : Prism Central IP (required)
    --pe-ip : Prism Element IP (optional)
    --config : Custom metrics configuration file
    """
    try:
      parser = argparse.ArgumentParser(
          description="Metrics Collection Framework Runner",
          epilog="""
        Examples:

        Specify custom config file:
          python runner.py  --config configs/metrics_config.json
                    """,
          formatter_class=argparse.RawDescriptionHelpFormatter
      )

      parser.add_argument(
          "--cvm_username",
          default="admin",
          help="PC/PE cvm username",
          required = True
      )

      parser.add_argument(
          "--pe_cvm_password",
          default="",
          help="PE cvm password",
          required=True
      )

      parser.add_argument(
          "--pc_cvm_password",
          default="",
          help="PC cvm password",
          required=True
      )

      parser.add_argument(
          "--ui_username",
          default="",
          help="PC/PE UI username",
          required=True
      )

      parser.add_argument(
          "--ui_password",
          default="",
          help="PC/PE UI password",
          required=True
      )


      parser.add_argument(
          "--config",
          default="api_metrics_catalog.json",
          help="Metrics config file"
      )

      self.args = parser.parse_args()

      return self.args

    except Exception as err:
      LOGGER.exception("Failed parsing command line arguments: %s", err)
      raise

  @EntryExit
  def set_environment_variables(self, testbed_config):
    """
    Set environment variables for PC and PE systems.

    Parameters
    ----------
    testbed_config : dict
        Configuration containing PC and PE IP details.
    """
    try:
      self.pc_ips.extend(v["ip"] for v in testbed_config.get("pcs", []))
      self.pe_ips.extend(v["ip"] for v in testbed_config.get("pes", []))

      if self.pc_ips:
        os.environ["PC_IPS"] = ",".join(self.pc_ips)

      if self.pe_ips:
        os.environ["PE_IPS"] = ",".join(self.pe_ips)

      os.environ["PC_CVM_USERNAME"] = self.args.cvm_username
      os.environ["PE_CVM_USERNAME"] = self.args.cvm_username
      os.environ["PC_CVM_PASSWORD"] = self.args.pc_cvm_password
      os.environ["PE_CVM_PASSWORD"] = self.args.pe_cvm_password
      os.environ["UI_USERNAME"] = self.args.ui_username
      os.environ["UI_PASSWORD"] = self.args.ui_password

      LOGGER.info("Environment variables set")

    except Exception as err:
      LOGGER.exception("Failed setting environment variables: %s", err)
      raise

  @EntryExit
  def run(self):
    """
    Main execution workflow of the runner.

    Steps performed:
    1. Parse CLI arguments
    2. Load testbed configuration
    3. Set environment variables
    4. Persist PC/PE cluster information
    5. Load metrics configuration
    6. Start metric collection threads
    7. Wait for all threads to complete
    8. Close database worker
    """
    try:
      self.args = self.parse_args()
      testbed_config = self.metric_processor.load_config(
          self.endpoint_config
      )
      self.set_environment_variables(testbed_config)
      self.metric_processor.persist_pc_pe_info_to_db()
      metric_config = self.metric_processor.load_config(
          self.metrics_config
      )

      cli_config = self.metric_processor.load_config(
          self.cli_config
      )

      dynamic_value_config = self.metric_processor.fetch_dynamic_values(metric_config)

      threads = []

      #CLI Threads
      for table_name, table_config in cli_config.items():
          config_chunk = {table_name: table_config}

          t = threading.Thread(
              target=self.cli_processor.process_data,
              args=(config_chunk,)
          )
          t.start()
          threads.append(t)

      #API Threads
      for table_name, table_config in dynamic_value_config.items():
        config_chunk = {table_name: table_config}
        thread = threading.Thread(
            target=self.metric_processor.process_data,
            args=(config_chunk,)
        )
        thread.start()
        threads.append(thread)

      for thread in threads:
        thread.join()
      LOGGER.info("All metric processing threads completed")
      # 2. WAIT for DB worker queue to drain
      while self.metric_processor.db_worker.queue_size != 0:
          time.sleep(0.1)
      self.metric_processor.db_worker.export_schema()

    except Exception as err:
      LOGGER.exception("Runner execution failed: %s", err)
      raise


if __name__ == "__main__":
  try:
    runner = Runner()
    runner.run()
  except Exception as err:
    LOGGER.exception("Fatal error in runner: %s", err)
    raise
