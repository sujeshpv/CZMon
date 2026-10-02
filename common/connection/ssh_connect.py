from library.const import *
from common.exceptions.exceptions import *
import paramiko
import logging
import atexit
import time
import os

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
DEFAULT_SSH_KEY_PATH = os.path.join(_PROJECT_ROOT, SSH, KEYS, NUTANIX)
# CVMs run sshd with MaxSessions 1, so a new command channel is refused until
# the server has finished closing the previous one.
CHANNEL_OPEN_ATTEMPTS = 10
CHANNEL_OPEN_RETRY_SECS = 0.5


class Ssh:
  """
  SSH utility class for managing connections and executing commands.

  Features
  --------
  - Supports password and key-based authentication
  - Retries SSH connection automatically
  - Executes remote commands
  - Cleans up SSH session on exit
  """

  def __init__(
      self,
      remote_ip,
      username,
      password=None,
      key_path=DEFAULT_SSH_KEY_PATH
  ):
    """
    Initialize SSH connection handler.

    Parameters
    ----------
    remote_ip : str
        Remote host IP address
    username : str
        SSH username
    password : str, optional
        Password for authentication
    key_path : str, optional
        Path to private SSH key
    """
    try:
      self.remote_ip = remote_ip
      self.username = username
      self.password = password
      self.key_path = key_path
      self.ssh_handle = None
      self._connect_with_retry()
      atexit.register(self.close_session)
    except Exception as err:
      error = CZMonError(
        "SSH connect failed",
        cause=err,
        context={
          "remote_ip": remote_ip,
          "username": username,
        },
      )
      LOG.error(error)
      raise error

  def _connect_with_retry(self):
    """
    Attempt SSH connection with retries.

    Tries up to 5 times before failing.

    Raises
    ------
    CZMonError
        If all retry attempts fail
    """
    for attempt in range(5):
      try:
        self.connect()
        if self.ssh_handle:
          return
      except Exception as err:
        error = CZMonError(
          "SSH connection attempt failed",
          cause=err,
          context={
            "attempt": attempt + 1,
            "remote_ip": self.remote_ip,
          },
        )
        LOG.error(error)
      time.sleep(2)
    error = CZMonError(
      "SSH connection retry exhausted",
      context={"remote_ip": self.remote_ip}
    )
    LOG.error(error)
    raise error

  def connect(self):
    """
    Establish SSH connection to remote host.

    Supports both key-based and password authentication.

    Raises
    ------
    CZMonError
        If connection fails
    """
    try:
      self.ssh_handle = paramiko.SSHClient()
      self.ssh_handle.set_missing_host_key_policy(
        paramiko.AutoAddPolicy()
      )
      if self.key_path:
        self.ssh_handle.connect(
          hostname=self.remote_ip,
          username=self.username,
          key_filename=self.key_path,
          timeout=10
        )
      elif self.password:
        self.ssh_handle.connect(
          hostname=self.remote_ip,
          username=self.username,
          password=self.password,
          timeout=10
        )
      else:
        raise CZMonError(
          "No authentication method provided",
          context={"remote_ip": self.remote_ip}
        )
      LOG.info("Login to host %s successful", self.remote_ip)
    except Exception as err:
      error = CZMonError(
        "SSH connection failed",
        cause=err,
        context={"remote_ip": self.remote_ip}
      )
      LOG.error(error)
      raise error

  def execute(self, cmd):
    """
    Execute command on remote host.

    Parameters
    ----------
    cmd : str or list
        Command to execute

    Returns
    -------
    str
        Command output

    Raises
    ------
    CZMonError
        If session is invalid or execution fails
    """
    try:
      if isinstance(cmd, (list, tuple)):
        cmd = " ".join(cmd)
      exit_status, output, error_output = self.execute_with_status(cmd)
      if exit_status == 0:
        return output.strip()
      raise CZMonError(
        "SSH command execution failed",
        context={
          "remote_ip": self.remote_ip,
          "command": cmd,
          "error": error_output.strip(),
        },
      )
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "SSH execution exception",
        cause=err,
        context={
          "remote_ip": self.remote_ip,
          "command": cmd,
        },
      )
      LOG.error(error)
      raise error

  def execute_with_status(self, cmd, timeout=None):
    """
    Execute command on remote host without raising on non-zero exit.

    Parameters
    ----------
    cmd : str or list
        Command to execute
    timeout : float, optional
        Seconds to wait for the command to exit. None waits indefinitely.

    Returns
    -------
    tuple
        (exit_code, stdout, stderr); stdout and stderr are not stripped.

    Raises
    ------
    CZMonTimeoutError
        If the command does not exit within timeout
    CZMonError
        If session is invalid or the command cannot be executed
    """
    try:
      if isinstance(cmd, (list, tuple)):
        cmd = " ".join(cmd)
      if not self.ssh_handle:
        raise CZMonError(
          "SSH session not established",
          context={"remote_ip": self.remote_ip}
        )
      for attempt in range(CHANNEL_OPEN_ATTEMPTS):
        try:
          stdin, stdout, stderr = self.ssh_handle.exec_command(
            cmd, timeout=timeout
          )
          break
        except paramiko.ChannelException:
          if attempt == CHANNEL_OPEN_ATTEMPTS - 1:
            raise
          time.sleep(CHANNEL_OPEN_RETRY_SECS)
      channel = stdout.channel
      if timeout is not None:
        deadline = time.monotonic() + timeout
        while not channel.exit_status_ready():
          if time.monotonic() >= deadline:
            channel.close()
            raise CZMonTimeoutError(
              "SSH command timed out",
              context={
                "remote_ip": self.remote_ip,
                "command": cmd,
                "timeout_secs": timeout,
              },
            )
          time.sleep(0.2)
      exit_status = channel.recv_exit_status()
      output = stdout.read().decode()
      error_output = stderr.read().decode()
      channel.close()
      return exit_status, output, error_output
    except Exception as err:
      if isinstance(err, CZMonError):
        raise
      error = CZMonError(
        "SSH execution exception",
        cause=err,
        context={
          "remote_ip": self.remote_ip,
          "command": cmd,
        },
      )
      LOG.error(error)
      raise error

  def close_session(self):
    """
    Close SSH session gracefully.

    This is automatically triggered on program exit.
    """
    try:
      if self.ssh_handle:
        LOG.info("Closing SSH session")
        self.ssh_handle.close()
    except Exception as err:
      error = CZMonError(
        "Error closing SSH session",
        cause=err,
        context={"remote_ip": self.remote_ip}
      )
      LOG.error(error)
      raise error
