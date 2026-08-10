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

    # --- Add these two new lines ---
    path("stats/", core_views.stats_view, name="stats"),
    path("api/stats-data/", core_views.stats_data_api, name="stats_data_api"),
]

