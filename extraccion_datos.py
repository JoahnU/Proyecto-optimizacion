"""
Script para descargar todos los datos necesarios del portal de TransMilenio
"""
import requests
import geopandas as gpd
import pandas as pd
import os
import zipfile
from config import Config, crear_directorios  # import module correctly (config.py)

def descargar_geojson(url, nombre_archivo):
    """Descarga archivo GeoJSON desde URL"""
    try:
        print(f"Descargando {nombre_archivo}...")
        gdf = gpd.read_file(url)
        ruta_guardado = os.path.join(Config.DATA_RAW, f"{nombre_archivo}.geojson")
        gdf.to_file(ruta_guardado, driver='GeoJSON')
        print(f"✓ {nombre_archivo} descargado: {len(gdf)} registros")
        return gdf
    except Exception as e:
        print(f"✗ Error descargando {nombre_archivo}: {e}")
        return None

def descargar_gtfs():
    """Descarga y extrae datos GTFS"""
    try:
        print("Descargando datos GTFS...")
        # Asegurar carpeta destino RAW
        os.makedirs(Config.DATA_RAW, exist_ok=True)

        ruta_zip = os.path.join(Config.DATA_RAW, "gtfs.zip")
        
        # Descargar archivo con comprobación de status
        response = requests.get(Config.URL_GTFS, stream=True, timeout=60)
        response.raise_for_status()
        total_bytes = 0
        with open(ruta_zip, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:  # evitar escribir chunks vacíos
                    f.write(chunk)
                    total_bytes += len(chunk)
        print(f"✓ GTFS descargado: {total_bytes} bytes en {ruta_zip}")
        
        # Verificar que sea un zip válido antes de extraer
        if not zipfile.is_zipfile(ruta_zip):
            raise ValueError(f"El archivo descargado no es un ZIP válido: {ruta_zip} (tamaño={total_bytes})")
        
        # Extraer archivo (asegurar carpeta destino)
        ruta_gtfs = os.path.join(Config.DATA_RAW, "gtfs")
        os.makedirs(ruta_gtfs, exist_ok=True)
        with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
            zip_ref.extractall(ruta_gtfs)
        
        print("✓ GTFS extraído en:", ruta_gtfs)
        return True
    except requests.exceptions.RequestException as re:
        print(f"✗ Error de red descargando GTFS: {re}")
        return False
    except zipfile.BadZipFile as bz:
        print(f"✗ ZIP corrupto: {bz}")
        return False
    except Exception as e:
        print(f"✗ Error descargando/extrayendo GTFS: {e}")
        return False

def main():
    """Función principal de descarga"""
    print("=== INICIANDO DESCARGA DE DATOS ===")
    
    # Crear directorios
    crear_directorios()
    
    # Descargar datasets geoespaciales
    zonas = descargar_geojson(Config.URL_ZONAS, "zonas_sitp")
    patios = descargar_geojson(Config.URL_PATIOS, "patios_sitp")
    rutas = descargar_geojson(Config.URL_RUTAS, "rutas_sitp")
    paraderos = descargar_geojson(Config.URL_PARADEROS, "paraderos_zonales")
    
    # Descargar GTFS
    descargar_gtfs()
    
    print("=== DESCARGA COMPLETADA ===")

if __name__ == "__main__":
    main()