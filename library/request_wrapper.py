"""
Generic REST API CRUD utility.
Provides create, read, update, delete operations.
"""

from typing import Dict, Any, Optional, List
from common.exceptions.exceptions import *
import requests
import logging
import urllib3
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
LOGGER = logging.getLogger(__name__)


class APICRUD:
  """
  Generic reusable REST API CRUD client.
  """
  def __init__(
    self,
    base_url: str,
    headers: Optional[Dict[str, str]] = None,
    auth: Optional[tuple] = None,
    timeout: int = 10,
    verify_ssl: bool = False,
  ):
    """
    Initialize the API client.
    """
    try:
      self.base_url = base_url.rstrip("/")
      self.headers = headers or {"Content-Type": "application/json"}
      self.auth = (
        os.environ.get("UI_USERNAME"),
        os.environ.get("UI_PASSWORD")
      )
      self.timeout = timeout
      self.verify_ssl = verify_ssl
    except Exception as err:
      error = CZMonError(
        "Failed initializing APICRUD client",
        cause=err,
        context={"base_url": base_url}
      )
      LOGGER.error(error)
      raise error

  def _request(
      self,
      method: str,
      endpoint: str,
      expected_status: Optional[List[int]] = None,
      **kwargs
  ) -> Any:
    """
    Internal request handler used by CRUD operations.
    """
    url = f"{self.base_url}/{endpoint.lstrip('/')}"
    try:
      response = requests.request(
        method=method,
        url=url,
        headers=self.headers,
        auth=self.auth,
        timeout=self.timeout,
        verify=self.verify_ssl,
        **kwargs
      )
      LOGGER.info(
        "API CALL | method=%s url=%s status=%s",
        method,
        url,
        response.status_code
      )
      if expected_status and response.status_code not in expected_status:
        error = CZMonError(
          "Unexpected API status code",
          context={
            "method": method,
            "url": url,
            "expected": expected_status,
            "actual": response.status_code,
            "response": response.text,
          }
        )
        LOGGER.error(error)
        raise error
      if response.text:
        try:
          return response.json()
        except ValueError:
          LOGGER.debug("Response is not JSON, returning raw text")
          return response.text
      return None
    except requests.exceptions.RequestException as err:
      error = CZMonError(
        "API request failed",
        cause=err,
        context={
          "method": method,
          "url": url,
        }
      )
      LOGGER.error(error)
      raise error
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Unexpected error during API request",
        cause=err,
        context={
          "method": method,
          "url": url,
        }
      )
      LOGGER.error(error)
      raise error

  def create(self, endpoint: str, data: Dict[str, Any]) -> Any:
    """
    Send a POST request to create a resource.
    """
    try:
      return self._request(
        "POST",
        endpoint,
        json=data,
        expected_status=[200, 201]
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Create request failed",
        cause=err,
        context={"endpoint": endpoint}
      )
      LOGGER.error(error)
      raise error

  def read(
      self,
      endpoint: str,
      params: Optional[Dict[str, Any]] = None
  ) -> Any:
    """
    Send a GET request to retrieve resources.
    """
    try:
      return self._request(
        "GET",
        endpoint,
        params=params,
        expected_status=[200]
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Read request failed",
        cause=err,
        context={"endpoint": endpoint}
      )
      LOGGER.error(error)
      raise error

  def update(
      self,
      endpoint: str,
      data: Dict[str, Any],
      method: str = "PUT"
  ) -> Any:
    """
    Update an existing resource.
    """
    try:
      return self._request(
        method,
        endpoint,
        json=data,
        expected_status=[200, 204]
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Update request failed",
        cause=err,
        context={
          "endpoint": endpoint,
          "method": method
        }
      )
      LOGGER.error(error)
      raise error

  def delete(self, endpoint: str) -> Any:
    """
    Send a DELETE request to remove a resource.
    """
    try:
      return self._request(
        "DELETE",
        endpoint,
        expected_status=[200, 202, 204]
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Delete request failed",
        cause=err,
        context={"endpoint": endpoint}
      )
      LOGGER.error(error)
      raise error
