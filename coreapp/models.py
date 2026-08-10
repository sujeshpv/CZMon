from django.db import models

class PrismGatewayStatus(models.Model):
  """Database model to store Prism Gateway heartbeat status."""
  ip_address = models.GenericIPAddressField(unique=True)
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = 'coreapp_prismgatewaystatus'

class VMPowerStates(models.Model):
  """Database model to store VM Counts and Affinity per host/SVM."""
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_vmpowerstates'

class TaskMonitor(models.Model):
  """Database model to track Acropolis task progress and completion status."""
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_taskmonitor'

class AHVHomeUsage(models.Model):
  """Database model to store AHV /home partition usage per cluster."""
  cluster_name = models.CharField(max_length=255)
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = 'coreapp_ahvhomeusage'

class UnderutilizedCluster(models.Model):
  """Database model to store Cluster CPU, Memory, and IOPS utilization."""
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_underutilizedcluster'
