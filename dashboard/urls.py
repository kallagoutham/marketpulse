from django.urls import path

from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.home, name="home"),
    path("architecture/", views.architecture, name="architecture"),
    path("producer/", views.producer, name="producer"),
    path(
        "producer/sample-template/",
        views.sample_dataset_template,
        name="sample_dataset_template",
    ),
    path("status/", views.status, name="status"),
]
