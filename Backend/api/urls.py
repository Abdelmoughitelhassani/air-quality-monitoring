from django.urls import path
from . import views

urlpatterns = [
    # Zones
    path('zones/', views.zones_list, name='zones-list'),
    path('zones/map_data/', views.map_data, name='zones-map-data'),
    path('zones/<str:zone_id>/history/', views.zone_history, name='zone-history'),
    
    # Mesures
    path('measurements/', views.measurements_list, name='measurements-list'),
    path('measurements/latest/', views.latest_measurements, name='measurements-latest'),
    path('measurements/stats/', views.stats, name='measurements-stats'),
]