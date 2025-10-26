"""
Configuración global del proyecto
"""
import os
from dataclasses import dataclass

@dataclass
class Config:
    # URLs de datos abiertos TransMilenio
    URL_ZONAS = "https://datosabiertos-transmilenio.hub.arcgis.com/api/download/v1/items/0e6721644ae54ebd8d28c70ee82e2ace/geojson?layers=18"
    URL_PATIOS = "https://datosabiertos-transmilenio.hub.arcgis.com/api/download/v1/items/1176a253e63a4de8a33332195a5d7b92/geojson?layers=1"
    URL_RUTAS = "https://datosabiertos-transmilenio.hub.arcgis.com/api/download/v1/items/6f412f25a90a4fa7b129b6aaa94e1965/geojson?layers=15"
    URL_PARADEROS = "https://datosabiertos.bogota.gov.co/dataset/5ba19d20-06af-4c04-b50c-8ecb9472327d/resource/624bb288-2a6d-466f-801a-93e5497cd879/download/paraderos.json"
    URL_GTFS = "https://storage.googleapis.com/gtfs-estaticos/GTFS-2025-09-17.zip"
    
    # Directorios
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_RAW = os.path.join(BASE_DIR, "data", "raw")
    DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")
    DATA_RESULTS = os.path.join(BASE_DIR, "data", "results")
    
    # Localidades objetivo
    LOCALIDADES = ["Bosa/Auto. Sur", "Kennedy/Las Américas"]
    ZONAS_DESTINO = ["Bosa", "Americas"]
    NUM_LOCALIDAD = [7,8]
    TIPO_RUTA = [3]

    
    # Configuración de optimización
    CAPACIDAD_PATIO_DEFAULT = 15  # Rutas por patio
    CRS_PROYECCION = "EPSG:3116"  # MAGNA-SIRGAS Colombia
    
    # Parámetros de distancia
    VELOCIDAD_PROMEDIO = 20  # km/h en hora pico

# Crear directorios
def crear_directorios():
    os.makedirs(Config.DATA_RAW, exist_ok=True)
    os.makedirs(Config.DATA_PROCESSED, exist_ok=True)
    os.makedirs(Config.DATA_RESULTS, exist_ok=True)

if __name__ == "__main__":
    crear_directorios()
    print("Directorios creados exitosamente")