from datetime import datetime, timezone
from common.logger.logger import EntryExit, setup_logger
from common.exceptions.exceptions import *
import sqlite3
import threading
import time
import uuid

try:
  import queue as Queue
except ImportError:
  import Queue

LOG = setup_logger(__name__)


class Sqlite3Worker(threading.Thread):
  """
  Thread-safe SQLite worker that processes database queries asynchronously.
  """

  def __init__(self, file_name, max_queue_size=100):
    """
    Initialize the SQLite worker thread.
    """
    try:
      threading.Thread.__init__(self)
      self.daemon = True
      self.sqlite3_conn = sqlite3.connect(
          file_name,
          check_same_thread=False,
          detect_types=sqlite3.PARSE_DECLTYPES
      )
      self.sqlite3_cursor = self.sqlite3_conn.cursor()
      self.sql_queue = Queue.Queue(maxsize=max_queue_size)
      self.results = {}
      self.max_queue_size = max_queue_size
      self.exit_set = False
      self.exit_token = str(uuid.uuid4())
      self.start()
      self.thread_running = True
    except Exception as err:
      error = CZMonError(
        "Failed to initialize SQLite worker",
        cause=err,
        context={"file_name": file_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def run(self):
    """
    Worker thread loop.
    """
    try:
      LOG.info("SQLite worker thread started")
      execute_count = 0
      for token, query, values in iter(self.sql_queue.get, None):
        if token != self.exit_token:
          self.run_query(token, query, values)
          execute_count += 1
          if self.sql_queue.empty() or execute_count == self.max_queue_size:
            self.sqlite3_conn.commit()
            execute_count = 0
        if self.exit_set and self.sql_queue.empty():
          LOG.info("Closing SQLite connection")
          self.sqlite3_conn.commit()
          self.sqlite3_conn.close()
          self.thread_running = False
          return
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError("Error in SQLite worker run loop", cause=err)
      LOG.error(error)
      raise error

  @EntryExit
  def run_query(self, token, query, values):
    """
    Execute a single SQL query.
    """
    try:
      if query.lower().strip().startswith(("select", "pragma")):
        try:
          self.sqlite3_cursor.execute(query, values)
          self.results[token] = self.sqlite3_cursor.fetchall()
        except sqlite3.Error as err:
          error = CZMonError(
            "SQL query execution failed",
            cause=err,
            context={"query": query}
          )
          LOG.error(error)
          self.results[token] = []
      else:
        try:
          self.sqlite3_cursor.execute(query, values)
        except sqlite3.Error as err:
          error = CZMonError(
            "SQL write execution failed",
            cause=err,
            context={"query": query}
          )
          LOG.error(error)
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "Unexpected error executing query",
        cause=err,
        context={"query": query}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def execute(self, query, values=None):
    """
    Queue a SQL query for execution.
    """
    try:
      if self.exit_set:
        return "Exit Called"
      values = values or []
      token = str(uuid.uuid4())
      if query.lower().strip().startswith(("select", "pragma")):
        self.sql_queue.put((token, query, values), timeout=5)
        return self.query_results(token)
      self.sql_queue.put((token, query, values), timeout=5)
    except Exception as err:
      error = CZMonError(
        "Failed to queue SQL query",
        cause=err,
        context={"query": query}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def query_results(self, token):
    """
    Wait and return results for a SELECT query.
    """
    try:
      delay = 0.001
      while True:
        if token in self.results:
          result = self.results[token]
          del self.results[token]
          return result
        time.sleep(delay)
        if delay < 8:
          delay += delay
    except Exception as err:
      error = CZMonError(
        "Error retrieving query results",
        cause=err,
        context={"token": token}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def close(self):
    """
    Gracefully terminate the SQLite worker thread.
    """
    try:
      LOG.info("Terminating SQLite worker")
      self.exit_set = True
      self.sql_queue.put((self.exit_token, "", ""), timeout=5)
      while self.thread_running:
        time.sleep(0.01)
    except Exception as err:
      error = CZMonError("Error closing SQLite worker", cause=err)
      LOG.error(error)
      raise error

  @property
  @EntryExit
  def queue_size(self):
    """
    Return the current size of the SQL queue.
    """
    try:
      return self.sql_queue.qsize()
    except Exception as err:
      error = CZMonError("Failed to get queue size", cause=err)
      LOG.error(error)
      raise error

  @EntryExit
  def table_exists(self, table_name):
    """
    Check whether a table exists.
    """
    try:
      query = """
      SELECT name FROM sqlite_master
      WHERE type='table' AND name=?
      """
      result = self.execute(query, (table_name,))
      return len(result) > 0
    except Exception as err:
      error = CZMonError(
        "Failed checking table existence",
        cause=err,
        context={"table": table_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def get_columns(self, table_name):
    try:
      query = f"PRAGMA table_info({table_name})"
      result = self.execute(query)
      return [row[1] for row in result]
    except Exception as err:
      error = CZMonError(
        "Failed retrieving columns",
        cause=err,
        context={"table": table_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def create_table(self, table_name):
    try:
      query = f"""
      CREATE TABLE IF NOT EXISTS {table_name} (
        created_at TEXT not null
      )
      """
      LOG.info("Creating table: %s", table_name)
      self.execute(query)
    except Exception as err:
      error = CZMonError(
        "Failed creating table",
        cause=err,
        context={"table": table_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def add_column(self, table_name, column_name):
    try:
      query = f"""
      ALTER TABLE {table_name}
      ADD COLUMN {column_name} TEXT
      """
      LOG.info("Adding column %s to %s", column_name, table_name)
      self.execute(query)
    except Exception as err:
      error = CZMonError(
        "Failed adding column",
        cause=err,
        context={"table": table_name, "column": column_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def ensure_schema(self, table_name, values):
    try:
      if not self.table_exists(table_name):
        self.create_table(table_name)
      existing_columns = self.get_columns(table_name)
      for column in values.keys():
        if "." in column:
          column = column.split(".")[-1]
        if column not in existing_columns:
          self.add_column(table_name, column)
    except Exception as err:
      error = CZMonError(
        "Failed ensuring schema",
        cause=err,
        context={"table": table_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def insert_row(self, table_name, values):
    try:
      cleaned_values = {
        key.split(".")[-1]: value for key, value in values.items()
      }
      cleaned_values["created_at"] = (
        datetime.now(timezone.utc).isoformat()
      )
      columns = ",".join(cleaned_values.keys())
      placeholders = ",".join(["?"] * len(cleaned_values))
      query = f"""
      INSERT INTO {table_name} ({columns})
      VALUES ({placeholders})
      """
      self.execute(query, list(cleaned_values.values()))
    except Exception as err:
      error = CZMonError(
        "Failed inserting row",
        cause=err,
        context={"table": table_name}
      )
      LOG.error(error)
      raise error

  @EntryExit
  def get_column_values(self, table_name, column_name):
    try:
      query = f"SELECT {column_name} FROM {table_name}"
      result = self.execute(query)
      return [row[0] for row in result] if result else []
    except Exception as err:
      error = CZMonError(
        "Failed retrieving column values",
        cause=err,
        context={"table": table_name, "column": column_name}
      )
      LOG.error(error)
      return []

  def export_schema(self):
    try:
      with open("library/schema.sql", "w") as f:
        for line in self.sqlite3_conn.iterdump():
          if line.startswith("CREATE"):
            f.write(f"{line}\n")
      LOG.info("schema.sql is generated.")
      self.sqlite3_conn.close()
    except Exception as err:
      error = CZMonError("Failed exporting schema", cause=err)
      LOG.error(error)
      raise error
