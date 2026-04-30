from collectors.api_processor import ApiProcessor
from collectors.cli_processor import CliProcessor
from common.logger.logger import EntryExit, setup_logger
from common.exceptions.exceptions import *
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
    try:
      self.args = None
      self.config = None
      self.pc_ips = []
      self.pe_ips = []
      self.endpoint_config = os.path.join(
          STATIC, CONFIGURATIONS, ENDPOINTS_JSON
      )
      self.api_config = os.path.join(
          STATIC, CONFIGURE, API_METRICS_CATALOG_JSON
      )
      self.cli_config = os.path.join(
          STATIC, CONFIGURE, CLI_METRICS_CATALOG_JSON
      )
      self.api_processor = ApiProcessor()
      self.cli_processor = CliProcessor()
    except Exception as err:
      error = CZMonError(
        "Failed initializing Runner",
        cause=err,
        context={}
      )
      LOGGER.error(error)
      raise error

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
          formatter_class=argparse.RawDescriptionHelpFormatter
      )
      parser.add_argument(
          "--run-type",
          help="Run type must be either 'api' or 'cli'",
          required=True,
          choices=["api", "cli"]
      )
      self.args = parser.parse_args()
      return self.args
    except Exception as err:
      error = CZMonError(
        "Failed parsing command line arguments",
        cause=err
      )
      LOGGER.error(error)
      raise error

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
      pcs = testbed_config.get("pcs", []) or []
      top_level_pes = testbed_config.get("pes", []) or []
      self.pc_ips = [node.get("ip") for zone in testbed_config for node in testbed_config[zone] if node.get("type") == "PC"]
      self.pe_ips = [node.get("ip") for zone in testbed_config for node in testbed_config[zone] if node.get("type") == "PE"]

      if self.pc_ips:
        os.environ["PC_IPS"] = ",".join(self.pc_ips)
      if self.pe_ips:
        os.environ["PE_IPS"] = ",".join(self.pe_ips)
      LOGGER.info("Environment variables set")
    except Exception as err:
      error = CZMonError(
        "Failed setting environment variables",
        cause=err,
        context={"testbed_config": testbed_config}
      )
      LOGGER.error(error)
      raise error

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
      testbed_config = self.api_processor.load_config(
          self.endpoint_config
      )
      self.set_environment_variables(testbed_config)
      self.api_processor.persist_pc_pe_info_to_db(testbed_config)
      metric_config = self.api_processor.load_config(
          self.api_config
      )
      cli_config = self.api_processor.load_config(
          self.cli_config
      )
      dynamic_value_config = self.api_processor.fetch_dynamic_values(
          metric_config
      )
      threads = []
      # CLI Threads
      if self.args.run_type.lower() == CLI:
        for table_name, table_config in cli_config.items():
          config_chunk = {table_name: table_config}
          t = threading.Thread(
              target=self.cli_processor.process_data,
              args=(config_chunk,)
          )
          t.start()
          threads.append(t)
      # API Threads
      if self.args.run_type == API:
        for table_name, table_config in dynamic_value_config.items():
          config_chunk = {table_name: table_config}
          thread = threading.Thread(
              target=self.api_processor.process_data,
              args=(config_chunk, testbed_config)
          )
          thread.start()
          threads.append(thread)
      for thread in threads:
        thread.join()
      LOGGER.info("All metric processing threads completed")
      while self.api_processor.db_worker.queue_size != 0:
        time.sleep(0.1)
      self.api_processor.db_worker.export_schema()
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Runner execution failed",
        cause=err
      )
      LOGGER.error(error)
      raise error

if __name__ == "__main__":
  try:
    runner = Runner()
    runner.run()
  except Exception as err:
    if isinstance(err, CZMonError):
      LOGGER.error(err)
      raise
    error = CZMonError(
      "Fatal error in runner",
      cause=err
    )
    LOGGER.error(error)
    raise error
