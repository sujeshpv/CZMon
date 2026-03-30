from library.request_wrapper import APICRUD
from common.connection.sqliteworker import Sqlite3Worker
from common.logger.logger import EntryExit, setup_logger
from common.exceptions.exceptions import *
from library.const import *
from library.urls import *
import os
import logging
import jmespath
import json

LOGGER = setup_logger(__name__)


class ApiProcessor:
  def __init__(self):
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
    except Exception as err:
      error = CZMonError(
        "Failed initializing ApiProcessor",
        cause=err
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def get_base_url(self, endpoint_type, ip):
    try:
      if endpoint_type in ("PC", "PE"):
        return f"https://{ip}:9440"
      raise CZMonError(
        "Unsupported endpoint type",
        context={"endpoint_type": endpoint_type}
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Failed generating base URL",
        cause=err,
        context={"endpoint_type": endpoint_type, "ip": ip}
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def extract_values(self, response, value_paths):
    try:
      extracted = {}
      for path in value_paths:
        extracted[path] = jmespath.search(path, response)
      return extracted
    except Exception as err:
      error = CZMonError(
        "Failed extracting values",
        cause=err,
        context={"paths": value_paths}
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def process_data(self, config):
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
          raise CZMonError(
            "Unsupported endpoint_type",
            context={"endpoint_type": endpoint_type}
          )
        for ip in ips:
          try:
            base_url = self.get_base_url(endpoint_type, ip)
            api = APICRUD(base_url)
            crud_method = getattr(api, self.method_map[method])
            kwargs = {}
            if params:
              kwargs["params"] = params
            if data:
              kwargs["data"] = data
            response = (
              crud_method(api_endpoint, **kwargs)
              if kwargs else crud_method(api_endpoint)
            )
            values = self.extract_values(response, value_paths)
            values["output"] = json.dumps(response)
            self.db_worker.ensure_schema(table_name, values)
            self.db_worker.insert_row(table_name, values)
          except Exception as ip_err:
            error = CZMonError(
              "Failed processing API for IP",
              cause=ip_err,
              context={
                "ip": ip,
                "endpoint": api_endpoint,
                "method": method
              }
            )
            LOGGER.error(error)
            continue
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Error processing metrics data",
        cause=err
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def load_config(self, config_path):
    try:
      with open(config_path, "r") as f:
        return json.load(f)
    except Exception as err:
      error = CZMonError(
        "Failed loading config file",
        cause=err,
        context={"config_path": config_path}
      )
      LOGGER.error(error)
      raise error

  @EntryExit
  def persist_pc_pe_info_to_db(self):
    try:
      systems = {
          "PE": {
              "ips": os.environ.get("PE_IPS", "").split(","),
              "endpoint": CLUSTER_VERSION_PATH + CLUSTERS
          },
          "PC": {
              "ips": os.environ.get("PC_IPS", "").split(","),
              "endpoint": PC_CLUSTER_VERSION_PATH + CLUSTERS_LIST
          }
      }
      values = ["uuid", "name", "clusterExternalIPAddress", "fullVersion"]
      for system, config in systems.items():
        for ip in config["ips"]:
          try:
            base_url = self.get_base_url(system, ip)
            api = APICRUD(base_url)
            if system == "PE":
              api_response = api.read(endpoint=config["endpoint"])
              for entity in api_response.get("entities"):
                try:
                  output = {k: entity.get(k) for k in values}
                  existing_uuids = set(
                    self.db_worker.get_column_values(CLUSTER, "uuid")
                  )
                  uuid = output.get("uuid")
                  if uuid not in existing_uuids:
                    self.db_worker.ensure_schema(CLUSTER, output)
                    self.db_worker.insert_row(CLUSTER, output)
                except Exception as inner_err:
                  LOGGER.error(
                    CZMonError(
                      "Failed processing PE entity",
                      cause=inner_err,
                      context={"ip": ip}
                    )
                  )
            elif system == "PC":
              api_response = api.create(
                  endpoint=config["endpoint"],
                  data={"kind": "cluster"}
              )
              for entity in api_response["entities"]:
                try:
                  if PRISM_CENTRAL in entity["status"]["resources"]["config"]["service_list"]:
                    output = {
                      "uuid": entity["metadata"]["uuid"],
                      "name": entity["status"]["name"],
                      "clusterExternalIPAddress": entity["status"]["resources"]["network"]["external_ip"],
                      "fullVersion": entity["status"]["resources"]["config"]["build"]["full_version"],
                    }
                    existing_uuids = set(
                      self.db_worker.get_column_values(CLUSTER, "uuid")
                    )
                    if output["uuid"] not in existing_uuids:
                      self.db_worker.ensure_schema(CLUSTER, output)
                      self.db_worker.insert_row(CLUSTER, output)
                except Exception as inner_err:
                  LOGGER.error(
                    CZMonError(
                      "Failed processing PC entity",
                      cause=inner_err,
                      context={"ip": ip}
                    )
                  )
          except Exception as err:
            LOGGER.error(
              CZMonError(
                "Failed processing system",
                cause=err,
                context={"system": system, "ip": ip}
              )
            )
    except Exception as err:
      error = CZMonError(
        "Failed persisting PC/PE info",
        cause=err
      )
      LOGGER.error(error)
      raise error

  def fetch_dynamic_values(self, data):
    try:
      if isinstance(data, dict):
        return {k: self.fetch_dynamic_values(v) for k, v in data.items()}
      if isinstance(data, list):
        return [self.fetch_dynamic_values(i) for i in data]
      if isinstance(data, str) and data.startswith("$(") and data.endswith(")"):
        expression = data[2:-1]
        table_name, column_name, condition = expression.split("#")
        cond_column, cond_value = condition.split("=")
        query = f"""
          SELECT {column_name}
          FROM {table_name}
          WHERE {cond_column} = ?
        """
        result = self.db_worker.execute(query, [cond_value])
        return result[0][0] if result else None
      return data
    except Exception as err:
      error = CZMonError(
        "Failed resolving dynamic values",
        cause=err,
        context={"data": str(data)}
      )
      LOGGER.error(error)
      raise error
