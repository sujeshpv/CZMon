"""Thread safe sqlite3 interface."""

from datetime import datetime, timezone
from common.logger.logger import EntryExit, setup_logger
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

  This worker uses a queue-based architecture where SQL queries are pushed
  into a queue and executed sequentially by a dedicated background thread.
  It supports both read and write operations while maintaining thread safety.

  Attributes
  ----------
  sqlite3_conn : sqlite3.Connection
      SQLite database connection.
  sqlite3_cursor : sqlite3.Cursor
      Cursor used to execute queries.
  sql_queue : Queue
      Queue holding pending SQL operations.
  results : dict
      Stores results of SELECT queries keyed by token.
  exit_set : bool
      Flag to terminate worker thread.
  exit_token : str
      Special token used to signal worker shutdown.
  """

  def __init__(self, file_name, max_queue_size=100):
    """
    Initialize the SQLite worker thread.

    Parameters
    ----------
    file_name : str
        SQLite database file path.
    max_queue_size : int, optional
        Maximum size of the SQL queue.
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
      LOG.exception("Failed to initialize SQLite worker: %s", err)
      raise

  @EntryExit
  def run(self):
    """
    Worker thread loop.

    Continuously processes SQL queries from the queue and commits
    transactions periodically or when the queue becomes empty.
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
      LOG.exception("Error in SQLite worker run loop: %s", err)
      raise

  @EntryExit
  def run_query(self, token, query, values):
    """
    Execute a single SQL query.

    Parameters
    ----------
    token : str
        Unique identifier used to map results for SELECT queries.
    query : str
        SQL query string.
    values : list
        Values for parameterized query.
    """
    try:

      if query.lower().strip().startswith("select") or query.lower().strip().startswith("pragma"):

        try:
          self.sqlite3_cursor.execute(query, values)
          self.results[token] = self.sqlite3_cursor.fetchall()

        except sqlite3.Error as err:
          LOG.error("Query error: %s", err)
          self.results[token] = []

      else:

        try:
          self.sqlite3_cursor.execute(query, values)

        except sqlite3.Error as err:
          LOG.error("Query error: %s", err)

    except Exception as err:
      LOG.exception("Unexpected error executing query: %s", err)
      raise

  @EntryExit
  def execute(self, query, values=None):
    """
    Queue a SQL query for execution.

    Parameters
    ----------
    query : str
        SQL query to execute.
    values : list, optional
        Parameter values for the query.

    Returns
    -------
    list or None
        Query results for SELECT statements.
    """
    try:

      if self.exit_set:
        return "Exit Called"

      values = values or []
      token = str(uuid.uuid4())

      if query.lower().strip().startswith("select") or query.lower().strip().startswith("pragma"):

        self.sql_queue.put((token, query, values), timeout=5)
        return self.query_results(token)

      else:

        self.sql_queue.put((token, query, values), timeout=5)

    except Exception as err:
      LOG.exception("Failed to queue SQL query: %s", err)
      raise

  @EntryExit
  def query_results(self, token):
    """
    Wait and return results for a SELECT query.

    Parameters
    ----------
    token : str
        Token associated with query result.

    Returns
    -------
    list
        Query results.
    """
    try:
      delay = 0.001

      while True:

        if token in self.results:

          return_val = self.results[token]
          del self.results[token]
          return return_val

        time.sleep(delay)

        if delay < 8:
          delay += delay

    except Exception as err:
      LOG.exception("Error retrieving query results: %s", err)
      raise

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
      LOG.exception("Error closing SQLite worker: %s", err)
      raise

  @property
  @EntryExit
  def queue_size(self):
    """
    Return the current size of the SQL queue.

    Returns
    -------
    int
    """
    try:
      return self.sql_queue.qsize()
    except Exception as err:
      LOG.exception("Failed to get queue size: %s", err)
      raise

  # ----------------------------------------------------
  # Schema Helpers
  # ----------------------------------------------------

  @EntryExit
  def table_exists(self, table_name):
    """
    Check whether a table exists in the database.

    Parameters
    ----------
    table_name : str

    Returns
    -------
    bool
    """
    try:
      query = """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name=?
            """

      result = self.execute(query, (table_name,))
      return len(result) > 0

    except Exception as err:
      LOG.exception("Failed checking table existence: %s", err)
      raise

  @EntryExit
  def get_columns(self, table_name):
    """
    Retrieve column names for a given table.

    Parameters
    ----------
    table_name : str

    Returns
    -------
    list
    """
    try:
      query = f"PRAGMA table_info({table_name})"
      result = self.execute(query)
      return [row[1] for row in result]

    except Exception as err:
      LOG.exception("Failed retrieving columns for table %s: %s",
                    table_name, err)
      raise

  @EntryExit
  def create_table(self, table_name):
    """
    Create a table if it does not exist.

    Parameters
    ----------
    table_name : str
    """
    try:
      query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                created_at TEXT not null
            )
            """
      LOG.info("Creating table: %s", table_name)
      self.execute(query)

    except Exception as err:
      LOG.exception("Failed creating table %s: %s", table_name, err)
      raise

  @EntryExit
  def add_column(self, table_name, column_name):
    """
    Add a column to an existing table.

    Parameters
    ----------
    table_name : str
    column_name : str
    """
    try:
      query = f"""
            ALTER TABLE {table_name}
            ADD COLUMN {column_name} TEXT
            """

      LOG.info("Adding column %s to %s", column_name, table_name)
      self.execute(query)

    except Exception as err:
      LOG.exception("Failed adding column %s to %s: %s",
                    column_name, table_name, err)
      raise

  @EntryExit
  def ensure_schema(self, table_name, values):
    """
    Ensure table schema contains required columns.

    Parameters
    ----------
    table_name : str
    values : dict
        Dictionary containing column names to ensure.
    """
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
      LOG.exception("Failed ensuring schema for table %s: %s", table_name, err)
      raise

  @EntryExit
  def insert_row(self, table_name, values):
    """
    Insert a row into the specified table.

    Parameters
    ----------
    table_name : str
    values : dict
    """
    try:
      cleaned_values = {}

      for key, value in values.items():
        clean_key = key.split(".")[-1]
        cleaned_values[clean_key] = value

      cleaned_values["created_at"] = datetime.now(timezone.utc).isoformat()

      columns = ",".join(cleaned_values.keys())
      placeholders = ",".join(["?"] * len(cleaned_values))
      query = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            """
      self.execute(query, list(cleaned_values.values()))

    except Exception as err:
      LOG.exception("Failed inserting row into %s: %s", table_name, err)
      raise

  @EntryExit
  def get_column_values(self, table_name, column_name):
    """
    Retrieve all values from a specific column.

    Parameters
    ----------
    table_name : str
    column_name : str

    Returns
    -------
    list
    """
    try:
      query = f"SELECT {column_name} FROM {table_name}"
      result = self.execute(query)

      if not result:
        return []

      return [row[0] for row in result]

    except Exception as err:
      LOG.exception(
          "Failed retrieving column values from %s.%s: %s",
          table_name,
          column_name,
          err
      )
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
        LOG.exception(err)
