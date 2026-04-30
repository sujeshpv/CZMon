"""
URL configuration for czmon project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""

from django.contrib import admin
from django.urls import path
from django.views.generic import RedirectView
from coreapp import views as core_views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("settings/", core_views.settings_view, name="settings"),
    path("api/cluster-metrics/options/", core_views.cluster_metrics_options_api, name="cluster_metrics_options_api"),
    path("api/cluster-metrics/summary/", core_views.cluster_metrics_summary_api, name="cluster_metrics_summary_api"),
    path("api/cluster-metrics/partition-nodes/", core_views.partition_nodes_api, name="partition_nodes_api"),
    path("api/cluster-metrics/pe-partition-series/", core_views.pe_partition_series_api, name="pe_partition_series_api"),
    path("cluster-metrics/", core_views.cluster_metrics_view, name="cluster_metrics"),
    path("", core_views.dashboard, name="home"),
    path("pe/", RedirectView.as_view(pattern_name="home", permanent=True), name="pe_redirect"),
]
