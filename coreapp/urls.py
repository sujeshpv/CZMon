from django.urls import path
from . import views

urlpatterns = [
    # Core Observational Pages
    path("", views.dashboard, name="home"),
    path("home/", views.home, name="home_page"),
    path("pe/", views.dashboard, name="pe_dashboard"),
    path("cluster_metrics/", views.cluster_metrics_view, name="cluster_metrics"),
    path("settings/", views.settings_view, name="settings"),
    path("stats/", views.stats_view, name="stats"),
    path("api/stats-data/", views.stats_data_api, name="stats_data_api"),

    # REST APIs for Frontend Visualizations
    path(
        "api/pe-partition-series/",
        views.pe_partition_series_api,
        name="pe_partition_series_api",
    ),
    path(
        "api/partition-nodes/",
        views.partition_nodes_api,
        name="partition_nodes_api",
    ),
    path(
        "api/cluster-metrics-options/",
        views.cluster_metrics_options_api,
        name="cluster_metrics_options_api",
    ),
    path(
        "api/cluster-metrics-summary/",
        views.cluster_metrics_summary_api,
        name="cluster_metrics_summary_api",
    ),
]

