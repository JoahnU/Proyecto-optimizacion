"""
Procesamiento y filtrado de datos para Bosa y Kennedy
"""
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point
import os
from config import Config, crear_directorios
import json



class ProcesadorDatos:
    def __init__(self):
        self.config = Config()
        
    def cargar_datos(self):
        """Carga todos los datos descargados"""
        print("Cargando datos...")
        
        self.zonas = gpd.read_file(os.path.join(Config.DATA_RAW, "zonas_sitp.geojson"))
        self.patios = gpd.read_file(os.path.join(Config.DATA_RAW, "patios_sitp.geojson"))
        self.rutas = gpd.read_file(os.path.join(Config.DATA_RAW, "rutas_sitp.geojson"))
        self.paraderos = gpd.read_file(os.path.join(Config.DATA_RAW, "paraderos_zonales.geojson"))
        
        # Cargar GTFS
        self.gtfs_stops = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "stops.txt"))
        self.gtfs_routes = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "routes.txt"))
        self.gtfs_trips = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "trips.txt"))
        self.gtfs_stop_times = pd.read_csv(os.path.join(Config.DATA_RAW, "gtfs", "stop_times.txt"))
        
        print("✓ Datos cargados exitosamente")
    
    def filtrar_localidades(self):
        """Filtra datos para Bosa y Kennedy"""
        print("Filtrando datos por localidades...")
        
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
    
    def procesar_gtfs(self):
        """Procesa datos GTFS para identificar primeros paraderos"""
        print("Procesando datos GTFS...")
        
        # Encontrar primer paradero de cada viaje
        primeros_stops = self.gtfs_stop_times.sort_values(['trip_id', 'stop_sequence'])\
                          .groupby('trip_id').first().reset_index()
        
        # Cruzar con trips y routes
        trips_con_rutas = pd.merge(self.gtfs_trips, self.gtfs_routes, on='route_id')
        stops_con_info = pd.merge(primeros_stops, self.gtfs_stops, on='stop_id')
        
        # Combinar toda la información
        # Incluir route_short_name/route_long_name desde GTFS para facilitar el emparejamiento
        self.primeros_paraderos_gtfs = pd.merge(
            stops_con_info,
            trips_con_rutas[['trip_id', 'route_id', 'service_id', 'route_short_name', 'route_long_name']],
            on='trip_id'
        )
        
        # Convertir a GeoDataFrame
        geometry = [Point(lon, lat) for lon, lat in 
                   zip(self.primeros_paraderos_gtfs['stop_lon'], 
                       self.primeros_paraderos_gtfs['stop_lat'])]
        
        self.primeros_paraderos_gtfs = gpd.GeoDataFrame(
            self.primeros_paraderos_gtfs,
            geometry=geometry,
            crs="EPSG:4326"
        )
        
        print(f"✓ GTFS procesado: {len(self.primeros_paraderos_gtfs)} primeros paraderos identificados")
    
    def crear_matriz_distancias(self):
        """Crea matriz de distancias entre patios y primeros paraderos"""
        print("Calculando matriz de distancias...")
        
        # Proyectar a coordenadas planas para cálculos precisos
        patios_proj = self.patios_bk.to_crs(Config.CRS_PROYECCION)
        paraderos_proj = self.primeros_paraderos_gtfs.to_crs(Config.CRS_PROYECCION)
        
        # Construir índices auxiliares para emparejar rutas del shapefile con rutas GTFS
        # Mapeo: route_short_name -> lista de route_id (GTFS)
        gtfs_short_to_ids = {}
        if 'route_short_name' in self.primeros_paraderos_gtfs.columns:
            for rid, short in zip(self.primeros_paraderos_gtfs['route_id'], self.primeros_paraderos_gtfs['route_short_name']):
                if pd.isna(short):
                    continue
                gtfs_short_to_ids.setdefault(str(short).strip(), set()).add(rid)

        # Para cada ruta, encontrar el primer paradero más cercano
        self.distancias = {}
        
        for _, patio in patios_proj.iterrows():
            patio_id = patio['objectid']  # Ajustar según estructura real
            self.distancias[patio_id] = {}
            
            for _, ruta in self.rutas_bk.iterrows():
                route_id = ruta['route_id']  # id usado en la capa rutas_bk (probablemente numérico)

                # Intentos de emparejamiento con GTFS (heurísticos)
                paraderos_ruta = gpd.GeoDataFrame(columns=paraderos_proj.columns)

                # 1) Intentar emparejar por 'cod_ruta' (ej. 'P7-1' -> 'P7')
                cod_ruta = ruta.get('cod_ruta') or ruta.get('cod_linea') or ruta.get('abrevia')
                if cod_ruta is not None:
                    base = str(cod_ruta).split('-')[0].strip()
                    if base in gtfs_short_to_ids:
                        candidate_ids = list(gtfs_short_to_ids[base])
                        paraderos_ruta = paraderos_proj[paraderos_proj['route_id'].isin(candidate_ids)]

                # 2) Si no hay match, intentar emparejar por 'nom_ruta' contenida en route_long_name
                if paraderos_ruta.empty and 'nom_ruta' in ruta and 'route_long_name' in self.primeros_paraderos_gtfs.columns:
                    nom = str(ruta.get('nom_ruta') or '').lower()
                    if nom:
                        mask = self.primeros_paraderos_gtfs['route_long_name'].fillna('').str.lower().str.contains(nom)
                        candidate_ids = self.primeros_paraderos_gtfs.loc[mask, 'route_id'].unique().tolist()
                        if candidate_ids:
                            paraderos_ruta = paraderos_proj[paraderos_proj['route_id'].isin(candidate_ids)]

                # 3) Último intento: si el campo 'route_id' de rutas_bk ya coincide textualmente con GTFS
                if paraderos_ruta.empty:
                    # comparar como string también por si hay coincidencias fortuitas
                    mask_eq = paraderos_proj['route_id'].astype(str) == str(route_id)
                    paraderos_ruta = paraderos_proj[mask_eq]

                if len(paraderos_ruta) > 0:
                    # Calcular distancia al paradero más cercano
                    distancias = paraderos_ruta.geometry.distance(patio.geometry)
                    distancia_min = distancias.min() / 1000  # Convertir a km
                    self.distancias[patio_id][route_id] = float(distancia_min)
                else:
                    # Si no hay datos GTFS asociados, usar aproximación
                    distancia_aproximada = self._calcular_distancia_aproximada(patio, ruta)
                    self.distancias[patio_id][route_id] = float(distancia_aproximada)
        
        print("✓ Matriz de distancias calculada")
    
    def _calcular_distancia_aproximada(self, patio, ruta):
        """Calcula distancia aproximada cuando no hay datos GTFS"""
        try:
            # Usar el punto inicial de la ruta
            if hasattr(ruta.geometry, 'coords'):
                punto_inicio = Point(ruta.geometry.coords[0])
                distancia = patio.geometry.distance(punto_inicio) / 1000
                return distancia
        except:
            pass
        
        return 5.0  # Distancia por defecto en km
    
    def guardar_datos_procesados(self):
        """Guarda archivos procesados: capacidades por patio + cap_total"""
        print("Guardando datos procesados...")
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

def main():
    """Función principal de procesamiento"""
    print("=== PROCESAMIENTO DE DATOS ===")
    
    procesador = ProcesadorDatos()
    procesador.cargar_datos()
    procesador.filtrar_localidades()
    procesador.procesar_gtfs()
    procesador.crear_matriz_distancias()
    procesador.guardar_datos_procesados()
    
    print("=== PROCESAMIENTO COMPLETADO ===")

if __name__ == "__main__":
    main()