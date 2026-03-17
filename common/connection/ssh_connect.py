import paramiko
import logging
import atexit
import time

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Ssh:
  """
  SSH utility class that provides connection management and
  remote command execution using Paramiko.

  This class supports:
  - Automatic retry for SSH connection
  - Remote command execution
  - Automatic session cleanup on program exit
  """

  def __init__(self, remote_ip, username, password):
    """
    Initialize the SSH connection handler.

    Parameters
    ----------
    remote_ip : str
        IP address of the remote host.
    username : str
        SSH username.
    password : str
        SSH password.
    """
    try:
      self.remote_ip = remote_ip
      self.username = username
      self.password = password
      self.ssh_handle = None

      self._connect_with_retry()

      atexit.register(self.close_session)

    except Exception as err:
      LOG.exception("Failed to initialize SSH client: %s", err)
      raise

  def _connect_with_retry(self):
    """
    Attempt SSH connection with retries.

    Retries up to 5 times with a delay between attempts.
    Raises an exception if connection cannot be established.
    """
    try:
      for attempt in range(5):
        try:
          self.connect()

          if self.ssh_handle:
            return

        except Exception as err:
          LOG.error("Attempt %s failed: %s", attempt + 1, err)

        time.sleep(2)

      raise Exception("Unable to establish SSH connection after retries")

    except Exception as err:
      LOG.exception("SSH connection retries exhausted: %s", err)
      raise

  def connect(self):
    """
    Establish an SSH connection to the remote host.
    """
    try:
      self.ssh_handle = paramiko.SSHClient()

      self.ssh_handle.set_missing_host_key_policy(
          paramiko.AutoAddPolicy()
      )

      self.ssh_handle.connect(
          hostname=self.remote_ip,
          username=self.username,
          password=self.password,
          timeout=10
      )

      LOG.info("Login to host %s successful", self.remote_ip)

    except Exception as err:
      LOG.exception(
          "SSH connection failed for host %s: %s",
          self.remote_ip,
          err
      )
      raise

  def execute(self, cmd):
    """
    Execute a command on the remote host.

    Parameters
    ----------
    cmd : str or list
        Command to execute on the remote machine.

    Returns
    -------
    str
        Command output (stdout if success, stderr if failure).
    """
    try:
      if isinstance(cmd, (list, tuple)):
        cmd = " ".join(cmd)

      if not self.ssh_handle:
        raise Exception("SSH session not established")

      stdin, stdout, stderr = self.ssh_handle.exec_command(cmd)

      exit_status = stdout.channel.recv_exit_status()

      if exit_status == 0:
        return stdout.read().decode().strip()

      else:
        return stderr.read().decode().strip()

    except Exception as err:
      LOG.exception(
          "Failed executing command on host %s: %s",
          self.remote_ip,
          err
      )
      raise

  def close_session(self):
    """
    Close the SSH session gracefully.

    This method is automatically called when the program exits.
    """
    try:
      if self.ssh_handle:
        LOG.info("Closing SSH session")
        self.ssh_handle.close()

    except Exception as err:
      LOG.exception("Error closing SSH session: %s", err)
      raise
