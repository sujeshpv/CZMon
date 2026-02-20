from django.urls import path
from . import views

urlpatterns = [
    path("", views.dashboard, name="home"),
    path("home/", views.home, name="home_page"),
    path("pe/", views.dashboard, name="pe_dashboard"),
]