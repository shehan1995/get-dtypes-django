from django.urls import path

from . import views

# URL config
urlpatterns = [
    path('process/', views.file_handler, name='upload_file'),
]