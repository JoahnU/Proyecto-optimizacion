"""
optimizacion_modelo.py
Modelo de optimización para asignación buses-patios con datos GTFS
"""
import pandas as pd
import numpy as np
import geopandas as gpd
from scipy.spatial.distance import cdist
import pulp
from config import Config
import json
import os
import re
from datetime import datetime, timedelta

class OptimizadorBusesPatios:
    def __init__(self):
        self.config = Config()
        self.modelo = None
        self.rutas_info = {}  # Información detallada de rutas
        self.patios_info = {}  # Información detallada de patios
        
    def cargar_datos_procesados(self):
        """Carga todos los datos necesarios incluyendo GTFS"""
        print("Cargando datos procesados...")
        
        try:
            # Cargar datos geoespaciales
            self.patios = gpd.read_file("data/processed/patios_bk.geojson")
            self.rutas = gpd.read_file("data/processed/rutas_bk.geojson")
            
            # Asegurar que estén en el mismo CRS
            self.patios = self.patios.to_crs(Config.CRS_PROYECCION)
            self.rutas = self.rutas.to_crs(Config.CRS_PROYECCION)
            
            # Cargar capacidades
            with open("data/processed/capacidades_patios.json", 'r') as f:
                capacidades_data = json.load(f)
                self.capacidades = capacidades_data["capacidades"]
                
            print(f"✓ {len(self.patios)} patios cargados")
            print(f"✓ {len(self.rutas)} rutas cargadas")
            
            # Cargar y procesar datos GTFS
            self.cargar_gtfs()
            
            return True
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def cargar_gtfs(self):
        """Carga y procesa datos GTFS para obtener frecuencia de rutas"""
        print("Cargando datos GTFS...")
        
        try:
            # Cargar archivos GTFS
            routes = pd.read_csv("data/raw/gtfs/routes.txt")
            trips = pd.read_csv("data/raw/gtfs/trips.txt")
            stop_times = pd.read_csv("data/raw/gtfs/stop_times.txt")
            calendar = pd.read_csv("data/raw/gtfs/calendar.txt")
            
            print(f"✓ GTFS cargado: {len(routes)} rutas, {len(trips)} viajes")
            
            # Filtrar para día laborable (usando calendar)
            laborable_service_ids = calendar[calendar['monday'] == 1]['service_id'].tolist()
            trips_laborables = trips[trips['service_id'].isin(laborable_service_ids)]
            
            # Calcular frecuencia por ruta (viajes por hora pico)
            # Asumimos hora pico: 7:00-9:00 AM
            stop_times['arrival_time'] = pd.to_timedelta(stop_times['arrival_time'])
            hora_pico_inicio = timedelta(hours=7)
            hora_pico_fin = timedelta(hours=9)
            
            viajes_hora_pico = stop_times[
                (stop_times['arrival_time'] >= hora_pico_inicio) & 
                (stop_times['arrival_time'] <= hora_pico_fin)
            ]['trip_id'].unique()
            
            trips_hora_pico = trips_laborables[trips_laborables['trip_id'].isin(viajes_hora_pico)]
            
            # Contar viajes por ruta en hora pico
            frecuencia_rutas = trips_hora_pico.groupby('route_id').size().reset_index(name='viajes_hora_pico')
            
            # Estimar número de buses necesarios por ruta
            # Asumimos que cada bus puede hacer 2 viajes por hora en hora pico
            viajes_por_bus_por_hora = 2
            frecuencia_rutas['buses_necesarios'] = np.ceil(
                frecuencia_rutas['viajes_hora_pico'] / (2 * viajes_por_bus_por_hora)  # 2 horas pico
            ).astype(int)
            
            # Mapear route_id GTFS a nuestras rutas
            self.frecuencia_rutas = frecuencia_rutas
            print("✓ Frecuencias de rutas calculadas:")
            print(frecuencia_rutas.describe())
            
            # Crear mapeo entre rutas GTFS y rutas geográficas
            self.crear_mapeo_rutas(routes)
            
            return True
            
        except Exception as e:
            print(f"⚠️ Error cargando GTFS, usando valores por defecto: {e}")
            # Valores por defecto si GTFS falla
            self.frecuencia_rutas = pd.DataFrame()
            return True
    
    def crear_mapeo_rutas(self, routes_gtfs):
        """Crea mapeo entre rutas GTFS y rutas geográficas"""
        print("Creando mapeo entre rutas...")
        
        # Aquí necesitarías lógica para mapear based on route codes o nombres
        # Por ahora, asumimos una correspondencia directa por índice
        
        self.rutas_con_buses = []
        
        for idx, ruta_geo in self.rutas.iterrows():
            ruta_id = f"ruta_{idx}"
            
            # Buscar correspondencia en GTFS (simplificado)
            if idx < len(self.frecuencia_rutas):
                buses_necesarios = self.frecuencia_rutas.iloc[idx]['buses_necesarios']
            else:
                # Valor por defecto si no hay datos GTFS
                buses_necesarios = 3  # buses por ruta por defecto
            
            self.rutas_con_buses.append({
                'ruta_id': ruta_id,
                'ruta_geo_idx': idx,
                'buses_necesarios': max(1, buses_necesarios),  # mínimo 1 bus
                'geometry': ruta_geo.geometry
            })
        
        print(f"✓ Mapeo creado: {len(self.rutas_con_buses)} rutas con buses asignados")
    
    def calcular_distancias(self):
        """Calcula matriz de distancias entre buses y patios"""
        print("Calculando matriz de distancias...")
        
        try:
            # Obtener centroides de patios y rutas
            if all(geom.geom_type == 'Point' for geom in self.patios.geometry):
                coords_patios = np.array([(geom.x, geom.y) for geom in self.patios.geometry])
            else:
                coords_patios = np.array([(geom.centroid.x, geom.centroid.y) for geom in self.patios.geometry])
            
            # Para cada ruta, usar su centroide
            coords_rutas = []
            self.rutas_ids_expandidas = []  # IDs expandidas por buses
            
            for ruta_info in self.rutas_con_buses:
                centroid = ruta_info['geometry'].centroid
                buses_necesarios = ruta_info['buses_necesarios']
                
                # Replicar coordenadas por cada bus necesario
                for bus_idx in range(buses_necesarios):
                    coords_rutas.append([centroid.x, centroid.y])
                    self.rutas_ids_expandidas.append(
                        f"{ruta_info['ruta_id']}_bus_{bus_idx}"
                    )
            
            coords_rutas = np.array(coords_rutas)
            
            print(f"Coordenadas patios: {coords_patios.shape}")
            print(f"Coordenadas buses: {coords_rutas.shape}")
            print(f"Total buses a asignar: {len(self.rutas_ids_expandidas)}")
            
            # Calcular matriz de distancias euclidianas en metros
            distancias = cdist(coords_rutas, coords_patios, metric='euclidean')
            
            # Convertir a kilómetros
            distancias_km = distancias / 1000.0
            
            # IDs de patios
            if 'objectid' in self.patios.columns:
                self.patios_ids = [f"patio_{int(row.objectid)}" for _, row in self.patios.iterrows()]
            else:
                self.patios_ids = [f"patio_{i}" for i in range(len(self.patios))]
            
            # Convertir a DataFrame
            self.distancias_df = pd.DataFrame(
                distancias_km,
                index=self.rutas_ids_expandidas,
                columns=self.patios_ids
            )
            
            print("✓ Matriz de distancias calculada")
            print(f"  - Rango distancias: {distancias_km.min():.2f} - {distancias_km.max():.2f} km")
            print(f"  - Buses totales: {len(self.rutas_ids_expandidas)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error calculando distancias: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def construir_modelo(self):
        """Construye el modelo de optimización considerando buses individuales"""
        print("Construyendo modelo de optimización...")
        
        try:
            # Crear modelo
            self.modelo = pulp.LpProblem("Asignacion_Buses_Patios", pulp.LpMinimize)
            
            # Conjuntos
            buses = self.rutas_ids_expandidas  # IDs de buses individuales
            patios = self.patios_ids
            
            print(f"Conjuntos: {len(buses)} buses, {len(patios)} patios")
            
            # Parámetros
            distancias = {(b, p): self.distancias_df.loc[b, p] 
                         for b in buses for p in patios}
            
            # Obtener capacidades reales de patios
            capacidades = {}
            for p in patios:
                patio_id = p.replace('patio_', '')
                capacidades[p] = self.capacidades.get(patio_id, Config.CAPACIDAD_PATIO_DEFAULT)
            
            # Parámetros económicos
            costo_km = 2500  # $/km (estimado)
            costo_operacion_patio = 1000000  # Costo fijo por patio utilizado ($)
            velocidad_kmh = Config.VELOCIDAD_PROMEDIO
            
            # Variables
            x = pulp.LpVariable.dicts("Asignacion", 
                                     [(b, p) for b in buses for p in patios],
                                     cat='Binary')
            
            y = pulp.LpVariable.dicts("Patio_Utilizado", patios, cat='Binary')
            t_max = pulp.LpVariable("Tiempo_Maximo", lowBound=0)
            
            # FUNCIÓN OBJETIVO - MINIMIZAR COSTOS TOTALES
            # Término 1: Costo variable por distancia recorrida
            costo_desplazamiento = pulp.lpSum(
                distancias[b, p] * costo_km * 2 * x[b, p]  # x2 para ida y vuelta
                for b in buses for p in patios
            )
            
            # Término 2: Costo fijo por patio utilizado
            costo_patios = pulp.lpSum(y[p] * costo_operacion_patio for p in patios)
            
            # Término 3: Penalización por tiempo máximo (equidad operativa)
            penalizacion_tiempo = t_max * 500000  # Peso alto para limitar tiempos excesivos
            
            self.modelo += costo_desplazamiento + costo_patios + penalizacion_tiempo
            
            # RESTRICCIONES
            
            # 1. Cada bus asignado a exactamente un patio
            for b in buses:
                self.modelo += pulp.lpSum(x[b, p] for p in patios) == 1, f"Asignacion_unica_{b}"
            
            # 2. No superar capacidad de patios
            for p in patios:
                self.modelo += pulp.lpSum(x[b, p] for b in buses) <= capacidades[p] * y[p], f"Capacidad_{p}"
            
            # 3. Tiempo máximo de desplazamiento (Big M formulation)
            M = 4  # 4 horas máximo razonable
            for b in buses:
                for p in patios:
                    tiempo_viaje = distancias[b, p] / velocidad_kmh
                    self.modelo += tiempo_viaje * x[b, p] <= t_max + M * (1 - x[b, p]), f"Tiempo_max_{b}_{p}"
            
            # 4. Buses de la misma ruta preferiblemente en el mismo patio
            # (Restricción blanda - opcional para mejorar operaciones)
            
            print("✓ Modelo construido:")
            print(f"  - {len(buses)} buses individuales")
            print(f"  - {len(patios)} patios") 
            print(f"  - {len(self.modelo.variables())} variables")
            print(f"  - {len(self.modelo.constraints)} restricciones")
            print(f"  - Capacidad total patios: {sum(capacidades.values())}")
            print(f"  - Buses totales: {len(buses)}")
            
            return True
            
        except Exception as e:
            print(f"❌ Error construyendo modelo: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def resolver(self):
        """Resuelve el modelo de optimización"""
        if self.modelo is None:
            print("❌ Modelo no construido")
            return False
            
        print("Resolviendo modelo...")
        
        try:
            # Resolver con CBC (gratuito)
            solver = pulp.PULP_CBC_CMD(timeLimit=600, msg=True, gapRel=0.05)
            self.modelo.solve(solver)
            
            print(f"✓ Estado: {pulp.LpStatus[self.modelo.status]}")
            if self.modelo.status == pulp.LpStatusOptimal:
                print(f"✓ Solución óptima encontrada")
                print(f"✓ Función objetivo: ${pulp.value(self.modelo.objective):,.0f}")
            else:
                print(f"⚠️ Estado de solución: {pulp.LpStatus[self.modelo.status]}")
            
            # Mostrar estadísticas
            self.mostrar_estadisticas()
            
            # Guardar resultados
            self.guardar_resultados()
            return True
            
        except Exception as e:
            print(f"❌ Error resolviendo modelo: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def mostrar_estadisticas(self):
        """Muestra estadísticas detalladas de la solución"""
        print("\n--- ESTADÍSTICAS DE LA SOLUCIÓN ---")
        
        if self.modelo.status != pulp.LpStatusOptimal:
            print("No hay solución óptima disponible")
            return
        
        # Análisis de asignaciones
        asignaciones_por_patio = {}
        buses_por_ruta_original = {}
        
        for var in self.modelo.variables():
            if var.name.startswith("Asignacion") and var.varValue == 1:
                bus_id, patio_id = self.parsear_nombre_variable(var.name)
                
                if bus_id and patio_id:
                    # Contar por patio
                    asignaciones_por_patio[patio_id] = asignaciones_por_patio.get(patio_id, 0) + 1
                    
                    # Agrupar por ruta original (sin el _bus_X)
                    ruta_original = bus_id.split('_bus_')[0]
                    buses_por_ruta_original[ruta_original] = buses_por_ruta_original.get(ruta_original, 0) + 1
        
        # Patios utilizados
        patios_utilizados = sum(1 for var in self.modelo.variables() 
                               if var.name.startswith("Patio_Utilizado") and var.varValue == 1)
        
        # Tiempo máximo
        t_max_val = next((var.varValue for var in self.modelo.variables() 
                         if var.name == "Tiempo_Maximo"), 0)
        
        print(f"Buses asignados: {sum(asignaciones_por_patio.values())}")
        print(f"Patios utilizados: {patios_utilizados}")
        print(f"Tiempo máximo de viaje: {t_max_val:.2f} horas ({t_max_val*60:.1f} minutos)")
        print(f"Costo total: ${pulp.value(self.modelo.objective):,.0f}")
        
        # Distribución detallada
        print("\nDistribución por patio:")
        for patio, count in sorted(asignaciones_por_patio.items()):
            capacidad = self.capacidades.get(patio.replace('patio_', ''), Config.CAPACIDAD_PATIO_DEFAULT)
            utilizacion = (count / capacidad) * 100
            print(f"  {patio}: {count:2d} buses ({utilizacion:5.1f}% de capacidad {capacidad})")
        
        print(f"\nRutas con buses asignados: {len(buses_por_ruta_original)}")
    
    def parsear_nombre_variable(self, var_name):
        """Parsea el nombre de variable de PuLP de forma robusta"""
        try:
            # Buscar patrones en el nombre de variable
            patterns = [
                r"'([^']+)_bus_(\d+)'.*'patio_([^']+)'",
                r"'([^']+)'.*'patio_([^']+)'",
                r"([^,_)]+)_bus_(\d+).*patio_([^,_)]+)",
                r"([^,_)]+).*patio_([^,_)]+)"
            ]
            
            for pattern in patterns:
                match = re.search(pattern, var_name)
                if match:
                    groups = match.groups()
                    if len(groups) >= 2:
                        bus_id = f"{groups[0]}_bus_{groups[1]}" if len(groups) > 2 else groups[0]
                        patio_id = f"patio_{groups[-1]}"
                        return bus_id, patio_id
            
            return None, None
            
        except Exception as e:
            print(f"Error parseando variable {var_name}: {e}")
            return None, None
    
    def guardar_resultados(self):
        """Guarda resultados detallados de la optimización"""
        print("Guardando resultados...")
        
        try:
            os.makedirs(Config.DATA_RESULTS, exist_ok=True)
            
            # Resultados detallados
            resultados_detallados = []
            resumen_por_ruta = {}
            
            for var in self.modelo.variables():
                if var.name.startswith("Asignacion") and var.varValue == 1:
                    bus_id, patio_id = self.parsear_nombre_variable(var.name)
                    
                    if bus_id and patio_id:
                        ruta_original = bus_id.split('_bus_')[0]
                        bus_num = bus_id.split('_bus_')[1] if '_bus_' in bus_id else "0"
                        
                        # Obtener distancia
                        distancia = self.distancias_df.loc[bus_id, patio_id] if (
                            bus_id in self.distancias_df.index and 
                            patio_id in self.distancias_df.columns
                        ) else 0
                        
                        resultado = {
                            'ruta_original': ruta_original,
                            'bus_id': bus_id,
                            'bus_num': bus_num,
                            'patio_id': patio_id,
                            'distancia_km': distancia,
                            'tiempo_minutos': (distancia / Config.VELOCIDAD_PROMEDIO) * 60,
                            'variable_name': var.name
                        }
                        resultados_detallados.append(resultado)
                        
                        # Agrupar por ruta para resumen
                        if ruta_original not in resumen_por_ruta:
                            resumen_por_ruta[ruta_original] = {
                                'patios_asignados': set(),
                                'total_buses': 0
                            }
                        resumen_por_ruta[ruta_original]['patios_asignados'].add(patio_id)
                        resumen_por_ruta[ruta_original]['total_buses'] += 1
            
            # Guardar resultados detallados
            if resultados_detallados:
                df_detallado = pd.DataFrame(resultados_detallados)
                df_detallado.to_csv(
                    os.path.join(Config.DATA_RESULTS, "asignaciones_detalladas.csv"), 
                    index=False, encoding='utf-8'
                )
                
                # Guardar resumen por ruta
                resumen_rutas = []
                for ruta, info in resumen_por_ruta.items():
                    resumen_rutas.append({
                        'ruta': ruta,
                        'patios_asignados': ', '.join(info['patios_asignados']),
                        'total_buses': info['total_buses'],
                        'concentracion': len(info['patios_asignados'])  # Menos es mejor
                    })
                
                df_resumen = pd.DataFrame(resumen_rutas)
                df_resumen.to_csv(
                    os.path.join(Config.DATA_RESULTS, "resumen_por_ruta.csv"), 
                    index=False, encoding='utf-8'
                )
                
                print(f"✓ Resultados guardados: {len(resultados_detallados)} asignaciones")
            
            self.guardar_resumen_modelo()
            print("✓ Todos los resultados guardados exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error guardando resultados: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def guardar_resumen_modelo(self):
        """Guarda resumen ejecutivo del modelo"""
        try:
            with open(os.path.join(Config.DATA_RESULTS, "resumen_ejecutivo.txt"), 'w', encoding='utf-8') as f:
                f.write("=== RESUMEN EJECUTIVO - ASIGNACIÓN BUSES PATIOS ===\n\n")
                f.write(f"Fecha de ejecución: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Estado: {pulp.LpStatus[self.modelo.status]}\n")
                f.write(f"Función objetivo: ${pulp.value(self.modelo.objective):,.0f}\n\n")
                
                f.write("ESTADÍSTICAS PRINCIPALES:\n")
                f.write(f"- Total buses asignados: {len(self.rutas_ids_expandidas)}\n")
                f.write(f"- Total patios disponibles: {len(self.patios_ids)}\n")
                f.write(f"- Capacidad total patios: {sum(self.capacidades.values())}\n")
                f.write(f"- Utilización global: {(len(self.rutas_ids_expandidas)/sum(self.capacidades.values()))*100:.1f}%\n")
                
            print("✓ Resumen ejecutivo guardado")
            
        except Exception as e:
            print(f"Error guardando resumen: {e}")

def main():
    """Función principal de optimización"""
    print("=== OPTIMIZACIÓN: ASIGNACIÓN BUSES-PATIOS CON GTFS ===")
    
    from config import crear_directorios
    crear_directorios()
    
    optimizador = OptimizadorBusesPatios()
    
    steps = [
        ("Cargando datos", optimizador.cargar_datos_procesados),
        ("Calculando distancias", optimizador.calcular_distancias),
        ("Construyendo modelo", optimizador.construir_modelo),
        ("Resolviendo modelo", optimizador.resolver)
    ]
    
    for step_name, step_func in steps:
        print(f"\n--- {step_name} ---")
        try:
            success = step_func()
            if not success:
                print(f"❌ Error en {step_name}, deteniendo ejecución")
                return
        except Exception as e:
            print(f"❌ Error inesperado en {step_name}: {e}")
            import traceback
            traceback.print_exc()
            return
    
    print("\n=== OPTIMIZACIÓN COMPLETADA ===")

if __name__ == "__main__":
    main() 