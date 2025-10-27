"""
Procesamiento y filtrado de datos para Bosa y Kennedy
"""
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon
import os
from config import Config, crear_directorios
import json
from scipy.spatial.distance import cdist
from geopy.distance import geodesic

class ProcesadorDatos:
    def __init__(self):
        self.config = Config()

        
    def cargar_datos(self):
        """Carga todos los datos descargados"""
        print("Cargando datos...")
        
        try:
            self.zonas = gpd.read_file(os.path.join(Config.DATA_RAW, "zonas_sitp.geojson"))
            self.patios = gpd.read_file(os.path.join(Config.DATA_RAW, "patios_sitp.geojson"))
            self.rutas = gpd.read_file(os.path.join(Config.DATA_RAW, "rutas_sitp.geojson"))
            self.paraderos = gpd.read_file(os.path.join(Config.DATA_RAW, "paraderos_zonales.geojson"))
            
            print("✓ Datos geoespaciales cargados exitosamente")
        except Exception as e:
            print(f"❌ Error cargando datos geoespaciales: {e}")
            return False
        
        # Cargar GTFS
        try:
            self.gtfs_stops = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "stops.txt"))
            self.gtfs_routes = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "routes.txt"))
            self.gtfs_trips = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "trips.txt"))
            self.gtfs_stop_times = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "stop_times.txt"))
            self.gtfs_cargado = True
            print("✓ Datos GTFS cargados exitosamente")
        except FileNotFoundError:
            print("⚠️  Datos GTFS no encontrados, usando datos geográficos alternativos")
            self.gtfs_cargado = False
        except Exception as e:
            print(f"⚠️  Error cargando GTFS: {e}")
            self.gtfs_cargado = False
        
        return True
    
    def filtrar_localidades(self):
        """Filtra datos para Bosa y Kennedy"""
        print("Filtrando datos por localidades...")
        
        try:
            # Filtrar zonas de interés
            zonas_bk = self.zonas[self.zonas['zona'].isin(Config.LOCALIDADES)]
            
            # Filtrar patios en Bosa/Kennedy
            self.patios_bk = gpd.sjoin(
                self.patios, 
                zonas_bk[['zona', 'geometry']], 
                how='inner', 
                predicate='within'
            )
            
            # Filtrar paraderos en Bosa/Kennedy
            self.paraderos_bk = self.paraderos[self.paraderos['localidad_'].isin(Config.NUM_LOCALIDAD)].copy()
            
            # Identificar rutas que operan en Bosa/Kennedy
            self.rutas_bk = self.rutas[self.rutas['loc_dest'].isin(Config.NUM_LOCALIDAD) & self.rutas['tip_serv'].isin(Config.TIPO_RUTA)].copy()
            
            print(f"✓ Patios filtrados: {len(self.patios_bk)}")
            print(f"✓ Paraderos filtrados: {len(self.paraderos_bk)}")
            print(f"✓ Rutas filtradas: {len(self.rutas_bk)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error filtrando localidades: {e}")
            return False
    
    def guardar_datos_procesados(self):
        """Guarda todos los datos procesados"""
        print("Guardando datos procesados...")
        
        try:
            # Guardar datos geoespaciales procesados
            self.patios_bk.to_file(
                os.path.join(Config.DATA_PROCESSED, "patios_bk.geojson"),
                driver='GeoJSON'
            )
            
            self.paraderos_bk.to_file(
                os.path.join(Config.DATA_PROCESSED, "paraderos_bk.geojson"),
                driver='GeoJSON'
            )
            
            self.rutas_bk.to_file(
                os.path.join(Config.DATA_PROCESSED, "rutas_bk.geojson"),
                driver='GeoJSON'
            )
            
            # Guardar capacidades
            self._guardar_capacidades()
            
            print("✓ Todos los datos procesados guardados")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando datos: {e}")
            return False
    
    def _guardar_capacidades(self):
        """Guarda archivos procesados: capacidades por patio + cap_total"""
        print("Guardando capacidades...")
        
        try:
            # Buscar columna de capacidad en la capa de patios
            posibles_cols = ['cap_total']
            col_cap = next((c for c in posibles_cols if c in self.patios_bk.columns), None)

            capacidades = {}
            for _, row in self.patios_bk.iterrows():
                # campos comunes para id de patio
                patio_id = row.get('objectid') or row.get('OBJECTID') or row.get('id') or row.get('Id')
                if patio_id is None:
                    continue
                capacidad = Config.CAPACIDAD_PATIO_DEFAULT
                if col_cap is not None:
                    val = row[col_cap]
                    if pd.notna(val):
                        try:
                            capacidad = int(float(val))
                        except Exception:
                            capacidad = Config.CAPACIDAD_PATIO_DEFAULT
                capacidades[str(int(patio_id))] = capacidad

            cap_total = sum(capacidades.values())

            salida = {
                "capacidades": capacidades,
                "cap_total": cap_total
            }

            os.makedirs(Config.DATA_PROCESSED, exist_ok=True)
            ruta_out = os.path.join(Config.DATA_PROCESSED, "capacidades_patios.json")
            with open(ruta_out, 'w', encoding='utf-8') as f:
                json.dump(salida, f, indent=2, ensure_ascii=False)

            print(f"✓ Capacidades guardadas en {ruta_out} (cap_total={cap_total})")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando capacidades: {e}")
            return False

def main():
    """Función principal de procesamiento"""
    print("=== PROCESAMIENTO DE DATOS ===")
    
    procesador = ProcesadorDatos()
    
    # Ejecutar en secuencia con manejo de errores
    steps = [
        ("Cargando datos", procesador.cargar_datos),
        ("Filtrando localidades", procesador.filtrar_localidades),
        ("Guardando datos", procesador.guardar_datos_procesados)
    ]
    
    for step_name, step_func in steps:
        print(f"\n--- {step_name} ---")
        try:
            success = step_func()
            if not success:
                print(f"❌ Error en {step_name}, deteniendo ejecución")
                break
        except Exception as e:
            print(f"❌ Error inesperado en {step_name}: {e}")
            break
    else:
        print("=== PROCESAMIENTO COMPLETADO EXITOSAMENTE ===")

if __name__ == "__main__":
    main()
