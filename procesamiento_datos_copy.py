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

class MatrizDistanciasHibrida:
    def __init__(self):
        self.config = Config()
    
    def _obtener_coordenadas(self, geometry):
        """Obtiene coordenadas de cualquier tipo de geometría"""
        if isinstance(geometry, Point):
            return geometry.y, geometry.x
        else:
            # Para Polygon, LineString, etc., usar el centroide
            centroid = geometry.centroid
            return centroid.y, centroid.x
        
    def _clasificar_zona(self, coords):
        """Clasifica coordenadas en zonas de Bosa/Kennedy"""
        lat, lon = coords
        
        # Bosa aproximada
        if lat > 4.62 and lon < -74.17:
            return 'bosa_sur'
        elif lat > 4.62 and lon >= -74.17:
            return 'bosa_norte'
        # Kennedy aproximada
        elif lat <= 4.62 and lon < -74.14:
            return 'kennedy_occidente'
        else:
            return 'kennedy_oriente'
    
    def _calcular_factor_correccion(self, zona_origen, zona_destino, distancia):
        """Calcula factor de corrección basado en zonas y distancia"""
        # Factores base por tipo de zona
        factores = {
            'bosa_sur': 1.6,
            'bosa_norte': 1.4,
            'kennedy_occidente': 1.5,
            'kennedy_oriente': 1.3
        }
        
        # Factor promedio entre origen y destino
        factor_promedio = (factores[zona_origen] + factores[zona_destino]) / 2
        
        # Ajustar por distancia (viajes más largos tienen mejor relación)
        if distancia > 10:
            factor_promedio *= 0.9
        elif distancia < 2:
            factor_promedio *= 1.2
        
        return min(max(factor_promedio, 1.2), 2.0)  # Límites razonables
    
    def _calcular_tiempo_viaje(self, distancia_km, zona_origen, zona_destino):
        """Calcula tiempo de viaje considerando congestión por zona"""
        # Velocidades promedio por zona (km/h)
        velocidades = {
            'bosa_sur': 15,
            'bosa_norte': 18,
            'kennedy_occidente': 16,
            'kennedy_oriente': 20
        }
        
        velocidad_promedio = (velocidades[zona_origen] + velocidades[zona_destino]) / 2
        
        # Tiempo en minutos
        tiempo = (distancia_km / velocidad_promedio) * 60
        
        # Añadir tiempo fijo por semáforos, paradas, etc.
        tiempo += distancia_km * 0.5  # 0.5 minutos extra por km
        
        return tiempo

    def calcular_matriz_hibrida(self, patios_gdf, rutas_gdf, primeros_paraderos):
        """Calcula matriz de distancias usando método híbrido mejorado"""
        print("Calculando matriz de distancias con método híbrido...")
        
        matriz_distancias = {}
        matriz_tiempos = {}
        
        # Convertir a coordenadas para cálculos
        coords_patios = []
        patio_ids = []
        for patio in patios_gdf.itertuples():
            try:
                coords = self._obtener_coordenadas(patio.geometry)
                coords_patios.append(coords)
                patio_id = getattr(patio, 'objectid', getattr(patio, 'OBJECTID', getattr(patio, 'id', None)))
                if patio_id is not None:
                    patio_ids.append(str(patio_id))
            except Exception as e:
                print(f"⚠️  Error procesando patio {getattr(patio, 'objectid', 'N/A')}: {e}")
                continue
        
        coords_paraderos = []
        ruta_ids = []
        for ruta_id, paradero in primeros_paraderos.items():
            try:
                coords = self._obtener_coordenadas(paradero['geometry'])
                coords_paraderos.append(coords)
                ruta_ids.append(ruta_id)
            except Exception as e:
                print(f"⚠️  Error procesando paradero para ruta {ruta_id}: {e}")
                continue
        
        print(f"✓ Procesando {len(coords_patios)} patios y {len(coords_paraderos)} rutas")
        
        if len(coords_patios) == 0 or len(coords_paraderos) == 0:
            print("❌ No hay suficientes datos para calcular la matriz")
            return {}, {}
        
        # Matriz de distancias lineales
        dist_lineales = np.zeros((len(coords_patios), len(coords_paraderos)))
        for i, coord_patio in enumerate(coords_patios):
            for j, coord_paradero in enumerate(coords_paraderos):
                dist_lineales[i][j] = geodesic(coord_patio, coord_paradero).kilometers
        
        # Aplicar factores de corrección basados en zona
        for i, patio_id in enumerate(patio_ids):
            if patio_id is None:
                continue
                
            matriz_distancias[str(patio_id)] = {}
            matriz_tiempos[str(patio_id)] = {}
            
            patio_coords = coords_patios[i]
            zona_patio = self._clasificar_zona(patio_coords)
            
            for j, ruta_id in enumerate(ruta_ids):
                paradero_coords = coords_paraderos[j]
                zona_paradero = self._clasificar_zona(paradero_coords)
                
                # Distancia lineal base
                dist_base = dist_lineales[i][j]
                
                # Aplicar factores de corrección
                factor_correccion = self._calcular_factor_correccion(zona_patio, zona_paradero, dist_base)
                distancia_corregida = dist_base * factor_correccion
                
                # Calcular tiempo considerando congestión
                tiempo_minutos = self._calcular_tiempo_viaje(distancia_corregida, zona_patio, zona_paradero)
                
                matriz_distancias[str(patio_id)][ruta_id] = round(distancia_corregida, 2)
                matriz_tiempos[str(patio_id)][ruta_id] = round(tiempo_minutos, 2)
        
        return matriz_distancias, matriz_tiempos

class ProcesadorDatos:
    def __init__(self):
        self.config = Config()
        self.calculador_distancias = MatrizDistanciasHibrida()
        
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
    
    def procesar_gtfs(self):
        """Procesa datos GTFS para identificar primeros paraderos o usa alternativa"""
        print("Procesando datos para primeros paraderos...")
        
        if self.gtfs_cargado:
            try:
                # Encontrar primer paradero de cada viaje
                primeros_stops = self.gtfs_stop_times.sort_values(['trip_id', 'stop_sequence'])\
                                  .groupby('trip_id').first().reset_index()
                
                # Cruzar con trips y routes
                trips_con_rutas = pd.merge(self.gtfs_trips, self.gtfs_routes, on='route_id')
                stops_con_info = pd.merge(primeros_stops, self.gtfs_stops, on='stop_id')
                
                # Combinar toda la información
                self.primeros_paraderos_gtfs = pd.merge(
                    stops_con_info,
                    trips_con_rutas[['trip_id', 'route_id', 'service_id']],
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
                return True
                
            except Exception as e:
                print(f"❌ Error procesando GTFS: {e}")
                self.gtfs_cargado = False
                return self._procesar_primeros_paraderos_alternativo()
        else:
            return self._procesar_primeros_paraderos_alternativo()
    
    def _procesar_primeros_paraderos_alternativo(self):
        """Método alternativo cuando no hay GTFS - asocia paraderos a rutas por proximidad"""
        print("Usando método alternativo para primeros paraderos...")
        
        self.primeros_paraderos_dict = {}
        
        try:
            for _, ruta in self.rutas_bk.iterrows():
                ruta_id = ruta.get('route_id') or ruta.get('ruta') or f"ruta_{ruta.name}"
                
                # Encontrar paradero más cercano al inicio de la ruta
                if hasattr(ruta.geometry, 'coords'):
                    try:
                        punto_inicio = Point(ruta.geometry.coords[0])
                    except:
                        # Si no puede obtener coords, usar centroide
                        punto_inicio = ruta.geometry.centroid
                    
                    # Encontrar paradero más cercano
                    distancia_min = float('inf')
                    paradero_cercano = None
                    
                    for _, paradero in self.paraderos_bk.iterrows():
                        try:
                            distancia = punto_inicio.distance(paradero.geometry)
                            if distancia < distancia_min:
                                distancia_min = distancia
                                paradero_cercano = paradero
                        except Exception as e:
                            continue
                    
                    if paradero_cercano is not None:
                        self.primeros_paraderos_dict[ruta_id] = {
                            'geometry': paradero_cercano.geometry,
                            'nombre': paradero_cercano.get('nombre', 'Desconocido'),
                            'localidad': paradero_cercano.get('localidad_', 'Desconocida')
                        }
            
            print(f"✓ Método alternativo: {len(self.primeros_paraderos_dict)} primeros paraderos identificados")
            return True
            
        except Exception as e:
            print(f"❌ Error en método alternativo: {e}")
            return False
    
    def crear_matriz_distancias(self):
        """Crea matriz de distancias usando el método híbrido mejorado"""
        print("Calculando matriz de distancias con método híbrido...")
        
        # Preparar datos para el cálculo híbrido
        if self.gtfs_cargado and hasattr(self, 'primeros_paraderos_gtfs'):
            # Convertir GTFS a formato de diccionario
            primeros_paraderos_dict = {}
            for _, row in self.primeros_paraderos_gtfs.iterrows():
                ruta_id = row['route_id']
                primeros_paraderos_dict[ruta_id] = {
                    'geometry': row['geometry'],
                    'nombre': row.get('stop_name', 'Desconocido')
                }
        else:
            primeros_paraderos_dict = getattr(self, 'primeros_paraderos_dict', {})
        
        if not primeros_paraderos_dict:
            print("❌ No hay primeros paraderos disponibles")
            return False
        
        # Filtrar rutas que tienen primeros paraderos definidos
        rutas_validas = []
        for _, ruta in self.rutas_bk.iterrows():
            ruta_id = self._obtener_ruta_id(ruta)
            if ruta_id in primeros_paraderos_dict:
                rutas_validas.append(ruta)
        
        if len(rutas_validas) < len(self.rutas_bk):
            print(f"⚠️  Advertencia: {len(self.rutas_bk) - len(rutas_validas)} rutas sin primer paradero definido")
        
        # Usar el método híbrido mejorado
        self.matriz_distancias, self.matriz_tiempos = self.calculador_distancias.calcular_matriz_hibrida(
            self.patios_bk, 
            self.rutas_bk, 
            primeros_paraderos_dict
        )
        
        print("✓ Matriz de distancias híbrida calculada")
        self._mostrar_estadisticas_matriz()
        return True
    
    def _obtener_ruta_id(self, ruta):
        """Obtiene el ID de ruta de diferentes formatos posibles"""
        return ruta.get('route_id') or ruta.get('ruta') or f"ruta_{ruta.name}"
    
    def _mostrar_estadisticas_matriz(self):
        """Muestra estadísticas de la matriz calculada"""
        todas_distancias = []
        todos_tiempos = []
        
        for patio_id, rutas in self.matriz_distancias.items():
            for ruta_id, distancia in rutas.items():
                todas_distancias.append(distancia)
                if patio_id in self.matriz_tiempos and ruta_id in self.matriz_tiempos[patio_id]:
                    todos_tiempos.append(self.matriz_tiempos[patio_id][ruta_id])
        
        if todas_distancias:
            print(f"\n=== ESTADÍSTICAS MATRIZ MEJORADA ===")
            print(f"Rutas procesadas: {len(todas_distancias)}")
            print(f"Distancias - Min: {min(todas_distancias):.2f} km, Max: {max(todas_distancias):.2f} km, Prom: {np.mean(todas_distancias):.2f} km")
            if todos_tiempos:
                print(f"Tiempos - Min: {min(todos_tiempos):.1f} min, Max: {max(todos_tiempos):.1f} min, Prom: {np.mean(todos_tiempos):.1f} min")
            
            # Distribución de distancias
            distancias_array = np.array(todas_distancias)
            print(f"Distribución: <2km: {np.sum(distancias_array < 2)} | 2-5km: {np.sum((distancias_array >= 2) & (distancias_array < 5))} | 5-10km: {np.sum((distancias_array >= 5) & (distancias_array < 10))} | >10km: {np.sum(distancias_array >= 10)}")
    
    def guardar_datos_procesados(self):
        """Guarda todos los datos procesados"""
        print("Guardando datos procesados...")
        
        try:
            # Guardar matrices de distancias y tiempos
            if hasattr(self, 'matriz_distancias'):
                with open(os.path.join(Config.DATA_PROCESSED, "matriz_distancias_hibrida.json"), 'w') as f:
                    json.dump(self.matriz_distancias, f, indent=2, ensure_ascii=False)
                
                with open(os.path.join(Config.DATA_PROCESSED, "matriz_tiempos_hibrida.json"), 'w') as f:
                    json.dump(self.matriz_tiempos, f, indent=2, ensure_ascii=False)
                
                print("✓ Matrices guardadas")
            
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
    print("=== PROCESAMIENTO DE DATOS CON MATRIZ HÍBRIDA ===")
    
    procesador = ProcesadorDatos()
    
    # Ejecutar en secuencia con manejo de errores
    steps = [
        ("Cargando datos", procesador.cargar_datos),
        ("Filtrando localidades", procesador.filtrar_localidades),
        ("Procesando GTFS", procesador.procesar_gtfs),
        ("Calculando matriz", procesador.crear_matriz_distancias),
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