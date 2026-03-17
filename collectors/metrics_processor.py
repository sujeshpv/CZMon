from library.request_wrapper import APICRUD
from common.connection.sqliteworker import Sqlite3Worker
from common.logger.logger import EntryExit, setup_logger
from library.const import *
from library.urls import *
import os
import logging
import jmespath
import json

LOGGER = setup_logger(__name__)


class MetricsProcessor:
  """
  MetricsProcessor is responsible for collecting metrics from Prism
  Central (PC) and Prism Element (PE) APIs and persisting them into
  a SQLite database using Sqlite3Worker.

  It dynamically processes API configurations, extracts required
  values using JMESPath expressions, and stores them into tables.
  """

  def __init__(self):
    """
    Initialize MetricsProcessor with database worker, HTTP method
    mappings, and system endpoint configurations.
    """
    try:
      self.db_worker = Sqlite3Worker("metrics.db")

      self.method_map = {
          "GET": "read",
          "POST": "create",
          "PUT": "update",
          "PATCH": "update",
          "DELETE": "delete",
      }

      self.method_arg_map = {
          "GET": "params",
          "POST": "data",
          "PUT": "data",
          "PATCH": "data",
          "DELETE": None
      }

      self.systems = {
          "PE": {
              "ips": os.environ.get("PE_IPS", "").split(","),
              "endpoint": CLUSTER_VERSION_PATH + CLUSTERS
          },
          "PC": {
              "ips": os.environ.get("PC_IPS", "").split(","),
              "endpoint": PC_CLUSTER_VERSION_PATH + CLUSTERS_LIST
          }
      }

    except Exception as err:
      LOGGER.exception("Failed initializing MetricsProcessor: %s", err)
      raise

  @EntryExit
  def get_base_url(self, endpoint_type, ip):
    """
    Build base URL for a given endpoint type and IP.

    Parameters
    ----------
    endpoint_type : str
        Either 'PC' or 'PE'.
    ip : str
        Target system IP.

    Returns
    -------
    str
        Base URL for API calls.
    """
    try:
      if endpoint_type == "PC":
        return f"https://{ip}:9440"

      if endpoint_type == "PE":
        return f"https://{ip}:9440"

    except Exception as err:
      LOGGER.exception("Failed generating base URL: %s", err)
      raise

  @EntryExit
  def extract_values(self, response, value_paths):
    """
    Extract required values from API response using JMESPath.

    Parameters
    ----------
    response : dict
        API response JSON.
    value_paths : list
        List of JMESPath expressions.

    Returns
    -------
    dict
        Extracted values mapped to their paths.
    """
    try:
      extracted = {}

      for path in value_paths:
        value = jmespath.search(path, response)
        extracted[path] = value

      return extracted

    except Exception as err:
      LOGGER.exception("Failed extracting values from response: %s", err)
      raise

  @EntryExit
  def process_data(self, config):
    """
    Process API configuration and persist extracted values into database.

    Parameters
    ----------
    config : dict
        Configuration describing API endpoints, methods, and
        JMESPath expressions for extracting values.
    """
    try:
      for table_name, entity_data in config.items():

        endpoint_type = entity_data.get("endpoint_type")
        api_endpoint = entity_data.get("api_endpoint")
        method = entity_data.get("http_method").upper()
        params = entity_data.get("params")
        data = entity_data.get("data")
        value_paths = entity_data.get("value_paths")

        if endpoint_type == "PC":
          ips = os.environ.get("PC_IPS", "").split(",")

        elif endpoint_type == "PE":
          ips = os.environ.get("PE_IPS", "").split(",")

        else:
          raise ValueError(f"Unsupported endpoint_type: {endpoint_type}")

        for ip in ips:

          base_url = self.get_base_url(endpoint_type, ip)
          api = APICRUD(base_url)

          crud_method = getattr(api, self.method_map[method])

          # build request arguments dynamically
          kwargs = {}

          if params:
            kwargs["params"] = params

          if data:
            kwargs["data"] = data

          response = crud_method(
              api_endpoint, **kwargs) if kwargs else crud_method(api_endpoint)

          values = self.extract_values(response, value_paths)

          self.db_worker.ensure_schema(table_name, values)

          self.db_worker.insert_row(table_name, values)

    except Exception as err:
      LOGGER.exception("Error processing metrics data: %s", err)
      raise

  @EntryExit
  def load_config(self, config_path):
    """
    Load metrics configuration from JSON file.

    Parameters
    ----------
    config_path : str
        Path to configuration JSON file.

    Returns
    -------
    dict
        Parsed configuration.
    """
    try:
      with open(config_path, "r") as f:
        config = json.load(f)

      return config

    except Exception as err:
      LOGGER.exception("Failed loading config file: %s", err)
      raise

  @EntryExit
  def persist_pc_pe_info_to_db(self):
    """
    Collect cluster information from PC and PE systems
    and store them in the database.

    Ensures duplicate UUIDs are not inserted.
    """
    try:
      values = ["uuid", "name", "clusterExternalIPAddress", "fullVersion"]

      self.systems = {
          "PE": {
              "ips": os.environ.get("PE_IPS", "").split(","),
              "endpoint": CLUSTER_VERSION_PATH + CLUSTERS
          },
          "PC": {
              "ips": os.environ.get("PC_IPS", "").split(","),
              "endpoint": PC_CLUSTER_VERSION_PATH + CLUSTERS_LIST
          }
      }

      for system, config in self.systems.items():

        for ip in config["ips"]:

          base_url = self.get_base_url(system, ip)

          api = APICRUD(base_url)

          if system == "PE":

            api_response = api.read(endpoint=config["endpoint"])

            for entity in api_response.get("entities"):

              output = {key: entity.get(key) for key in values}

              try:
                existing_uuids = set(
                    self.db_worker.get_column_values(CLUSTER, "uuid")
                )
              except Exception:
                existing_uuids = set()

              uuid = output.get("uuid")

              if uuid not in existing_uuids:

                self.db_worker.ensure_schema(CLUSTER, output)

                self.db_worker.insert_row(CLUSTER, output)

                existing_uuids.add(uuid)

          elif system == "PC":

            output = {}

            api_response = api.create(
                endpoint=config["endpoint"],
                data={"kind": "cluster"}
            )

            for entity in api_response["entities"]:

              if PRISM_CENTRAL in entity["status"]["resources"]["config"]["service_list"]:

                output["uuid"] = entity["metadata"]["uuid"]
                output["name"] = entity["status"]["name"]
                output["clusterExternalIPAddress"] = entity["status"]["resources"]["network"]["external_ip"]
                output["fullVersion"] = entity["status"]["resources"]["config"]["build"]["full_version"]

                try:
                  existing_uuids = set(
                      self.db_worker.get_column_values(CLUSTER, "uuid")
                  )
                except Exception:
                  existing_uuids = set()

                uuid = output.get("uuid")

                if uuid not in existing_uuids:

                  self.db_worker.ensure_schema(CLUSTER, output)

                  self.db_worker.insert_row(CLUSTER, output)

                  existing_uuids.add(uuid)

    except Exception as err:
      LOGGER.exception("Failed persisting PC/PE cluster information: %s", err)
      raise

  def fetch_dynamic_values(self, data):
      """
      Recursively resolve dynamic values inside a JSON structure.
      Looks for patterns like $(table#column#filter=value)
      and replaces them with DB values.
      """
      try:
          if isinstance(data, dict):
              return {
                  key: self.fetch_dynamic_values(value)
                  for key, value in data.items()
              }

          elif isinstance(data, list):
              return [
                  self.fetch_dynamic_values(item)
                  for item in data
              ]

          elif isinstance(data, str) and data.startswith("$(") and data.endswith(")"):

              expression = data[2:-1]

              table_name, column_name, condition = expression.split("#")
              cond_column, cond_value = condition.split("=")

              query = f"""
                SELECT {column_name}
                FROM {table_name}
                WHERE {cond_column} = ?
                """

              result = self.db_worker.execute(query, [cond_value])

              if result:
                  return result[0][0]

              return None

          else:
              return data

      except Exception as err:
          LOGGER.exception("Failed resolving dynamic values: %s", err)
          raise
