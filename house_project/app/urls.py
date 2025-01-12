from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('data/', views.get_data, name='get_data'),
    path('schema/', views.get_schema, name='get_schema'),
    path('count/', views.get_count, name='get_count'),
    path('search/', views.search, name='search'),
    path('cities/', views.get_cities, name='get_cities'),
    path('districts/', views.get_districts, name='get_districts'),
    path('price_distribution/', views.get_price_distribution, name='get_price_distribution'),
    path('area_distribution/', views.get_area_distribution, name='get_area_distribution'),
    path('charts/', views.charts, name='charts'),
]
