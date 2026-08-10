from django.db import models

class PrismGatewayStatus(models.Model):
  ip_address = models.GenericIPAddressField(unique=True)
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = 'coreapp_prismgatewaystatus'

class VMPowerStates(models.Model):
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_vmpowerstates'

class TaskMonitor(models.Model):
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_taskmonitor'

class AHVHomeUsage(models.Model):
  cluster_name = models.CharField(max_length=255)
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now=True)

  class Meta:
    db_table = 'coreapp_ahvhomeusage'

class UnderutilizedCluster(models.Model):
  ip_address = models.GenericIPAddressField()
  status_data = models.JSONField(null=True, blank=True)
  created_at = models.DateTimeField(auto_now_add=True)

  class Meta:
    db_table = 'coreapp_underutilizedcluster'
