from django.db import models

class PrismGatewayStatus(models.Model):
    ip_address = models.GenericIPAddressField(unique=True)
    status_data = models.JSONField(null=True, blank=True)  # Stores nosVersion, clusterFunction, siteType
    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'coreapp_prismgatewaystatus'

class VMPowerStates(models.Model):
    ip_address = models.GenericIPAddressField()  # Cluster IP
    status_data = models.JSONField(null=True, blank=True)  # Stores VM counts and affinity details
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'coreapp_vmpowerstates'

class TaskMonitor(models.Model):
    ip_address = models.GenericIPAddressField()
    status_data = models.JSONField(null=True, blank=True)  # Stores task list and their statuses
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'coreapp_taskmonitor'

class AHVHomeUsage(models.Model):
    cluster_name = models.CharField(max_length=255)
    status_data = models.JSONField(null=True, blank=True)  # Total, available, and usage % of partitions
    created_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'coreapp_ahvhomeusage'

class UnderutilizedCluster(models.Model):
    ip_address = models.GenericIPAddressField()
    status_data = models.JSONField(null=True, blank=True)  # Stores CPU%, Memory%, IOPS, and ALERT flag
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'coreapp_underutilizedcluster'

