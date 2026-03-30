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
    try:
      parser = argparse.ArgumentParser(
          description="Metrics Collection Framework Runner",
          formatter_class=argparse.RawDescriptionHelpFormatter
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
    try:
      self.pc_ips.extend(v["ip"] for v in testbed_config.get("pcs", []))
      self.pe_ips.extend(v["ip"] for v in testbed_config.get("pes", []))
      if self.pc_ips:
        os.environ["PC_IPS"] = ",".join(self.pc_ips)
      if self.pe_ips:
        os.environ["PE_IPS"] = ",".join(self.pe_ips)
      os.environ["UI_USERNAME"] = self.args.ui_username
      os.environ["UI_PASSWORD"] = self.args.ui_password
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
    try:
      self.args = self.parse_args()
      testbed_config = self.api_processor.load_config(
          self.endpoint_config
      )
      self.set_environment_variables(testbed_config)
      self.api_processor.persist_pc_pe_info_to_db()
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
              args=(config_chunk,)
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
