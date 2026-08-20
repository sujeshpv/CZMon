from django.db import models

class PrismGatewayStatus(models.Model):
  """Database model to store Prism Gateway heartbeat status."""
  ip_address = models.GenericIPAddressField(unique=True)
  is_online = models.BooleanField(default=False)
  status_data = models.JSONField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  def __str__(self):
    return f"{self.ip_address} - {'Online' if self.is_online else 'Offline'}"

class VmCountPerHost(models.Model):
  """Database model to store VM Counts and Affinity per host/SVM."""
  cluster_ip = models.GenericIPAddressField(unique=True)
  cluster_name = models.CharField(max_length=255, blank=True)
  status_data = models.JSONField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  def __str__(self):
    return f"{self.cluster_ip} ({self.cluster_name}) - Checked: {self.last_checked}"

class ClusterUtilization(models.Model):
  """Database model to store Cluster CPU, Memory, and IOPS utilization."""
  cluster_ip = models.GenericIPAddressField(unique=True)
  cpu_usage_percent = models.FloatField(null=True, blank=True)
  memory_usage_percent = models.FloatField(null=True, blank=True)
  iops = models.FloatField(null=True, blank=True)
  is_underutilized = models.BooleanField(default=False)
  error_message = models.TextField(null=True, blank=True)
  status_data = models.JSONField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  def __str__(self):
    return f"{self.cluster_ip} - Underutilized: {self.is_underutilized}"

class TimezoneHealthCheck(models.Model):
  """Database model to store Cluster Timezone (UTC) Health Check results."""
  cluster_ip = models.GenericIPAddressField(unique=True)
  endpoint_type = models.CharField(max_length=10, blank=True)
  status = models.CharField(max_length=20, blank=True)
  details_data = models.JSONField(null=True, blank=True)
  messages_data = models.JSONField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  def __str__(self):
    return f"{self.cluster_ip} ({self.endpoint_type}) - Status: {self.status}"

class CZAlert(models.Model):
  """Database model to store unresolved Prism Central Alerts."""
  pc_name_or_ip = models.CharField(max_length=255)
  alert_policy_id = models.CharField(max_length=255)
  source_cluster = models.CharField(max_length=255)
  alert_messages = models.JSONField(null=True, blank=True)
  error_message = models.TextField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  class Meta:
    unique_together = ('pc_name_or_ip', 'alert_policy_id', 'source_cluster')

  def __str__(self):
    return f"{self.pc_name_or_ip} - {self.source_cluster} ({self.alert_policy_id})"

class AhvHomeUsage(models.Model):
  """Database model to store AHV /home partition usage per cluster."""
  cluster_name = models.CharField(max_length=255, unique=True)
  status_data = models.JSONField(null=True, blank=True)
  last_checked = models.DateTimeField(auto_now=True)

  def __str__(self):
    return f"{self.cluster_name} - Checked: {self.last_checked}"




