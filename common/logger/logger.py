from logging.handlers import RotatingFileHandler
from datetime import datetime
import logging
import functools
import time
import traceback


def setup_logger(name):
  """
  Configure and return a logger instance.

  This logger supports:
  - Console logging for DEBUG, INFO, and ERROR levels
  - Rotating file logging with a maximum size of 10MB
  - Timestamped log file names

  Parameters
  ----------
  name : str
    Name of the logger (usually __name__ of the module)

  Returns
  -------
  logging.Logger
    Configured logger instance
  """
  try:
    logger = logging.getLogger(name)
    if logger.handlers:
      logger.propagate = False
      return logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
      "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    )
    # -------------------------
    # Console Handler
    # -------------------------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    class ConsoleFilter(logging.Filter):
      def filter(self, record):
        return record.levelno in (
          logging.DEBUG,
          logging.INFO,
          logging.WARNING,
          logging.ERROR
        )
    console_handler.addFilter(ConsoleFilter())
    console_handler.setFormatter(formatter)
    # -------------------------
    # Main File Handler
    # -------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"metrics_framework_{timestamp}.log"
    file_handler = RotatingFileHandler(
      log_file,
      maxBytes=10 * 1024 * 1024,
      backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    # -------------------------
    # FATAL File Handler
    # -------------------------
    fatal_file = f"metrics_fatal_{timestamp}.log"
    fatal_handler = RotatingFileHandler(
      fatal_file,
      maxBytes=10 * 1024 * 1024,
      backupCount=5
    )
    fatal_handler.setLevel(logging.ERROR)
    fatal_handler.setFormatter(formatter)
    # -------------------------
    # Add Handlers
    # -------------------------
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.addHandler(fatal_handler)
    logger.propagate = False
    return logger
  except Exception as err:
    logging.basicConfig(level=logging.DEBUG)
    logging.error("Failed to setup logger: %s", err)
    raise

LOGGER = setup_logger(__name__)


def EntryExit(func):
  """
  Decorator used to automatically log function entry,
  exit, execution time, and errors.

  It logs:
    - Function name
    - Arguments
    - Execution time
    - Return value
    - Stack trace if an exception occurs

  Parameters
  ----------
  func : function
  Function to wrap with logging.

  Returns
  -------
  function
    Wrapped function with entry/exit logging.
  """
  @functools.wraps(func)
  def wrapper(*args, **kwargs):
    logger = setup_logger(func.__module__)
    func_name = func.__name__

    try:
      logger.debug(
        "ENTER function=%s args=%s kwargs=%s",
        func_name,
        args,
        kwargs
      )
      start_time = time.time()
      result = func(*args, **kwargs)
      execution_time = round(time.time() - start_time, 4)
      logger.debug(
        "EXIT function=%s execution_time=%ss return=%s",
        func_name,
        execution_time,
        result
      )
      return result
    except Exception as err:
      logger.error(
        "ERROR in function=%s error=%s",
        func_name,
        err,
        exc_info=True
      )
      logger.error(
        "FATAL in function=%s error=%s",
        func_name,
        err,
        exc_info=True
      )
      raise
  return wrapper
