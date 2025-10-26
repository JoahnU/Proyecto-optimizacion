"""
Modelo de Optimización para Asignación Patio-Ruta SITP
Con diagnóstico de infactibilidad y relajación de restricciones
"""
import pulp
import pandas as pd
import numpy as np
import json
import os
from config import Config
import geopandas as gpd
from datetime import datetime

class DiagnosticoInfactibilidad:
    """Clase para diagnosticar y solucionar problemas de infactibilidad"""
    
    @staticmethod
    def verificar_factibilidad_basica(matriz_distancias, capacidades, rutas_ids):
        """Verifica factibilidad básica antes de crear el modelo"""
        print("\n=== DIAGNÓSTICO DE FACTIBILIDAD ===")
        
        # 1. Verificar que hay suficientes rutas y patios
        total_patios = len(matriz_distancias)
        total_rutas = len(rutas_ids)
        capacidad_total = sum(capacidades.values())
        
        print(f"Patios disponibles: {total_patios}")
        print(f"Rutas a asignar: {total_rutas}")
        print(f"Capacidad total: {capacidad_total}")
        
        if total_rutas > capacidad_total:
            print(f"❌ PROBLEMA: Capacidad insuficiente")
            print(f"   Rutas ({total_rutas}) > Capacidad ({capacidad_total})")
            return False
        
        # 2. Verificar que cada ruta tiene al menos un patio posible
        rutas_sin_patio = []
        for ruta_id in rutas_ids:
            patios_posibles = [p for p in matriz_distancias.keys() if ruta_id in matriz_distancias[p]]
            if not patios_posibles:
                rutas_sin_patio.append(ruta_id)
        
        if rutas_sin_patio:
            print(f"❌ PROBLEMA: {len(rutas_sin_patio)} rutas sin patio posible")
            print(f"   Ejemplos: {rutas_sin_patio[:5]}")
            return False
        
        # 3. Verificar capacidad por patio vs rutas posibles
        for patio_id, capacidad in capacidades.items():
            if patio_id in matriz_distancias:
                rutas_posibles = list(matriz_distancias[patio_id].keys())
                if len(rutas_posibles) < capacidad:
                    print(f"⚠️  Patio {patio_id}: Capacidad ({capacidad}) > Rutas posibles ({len(rutas_posibles)})")
        
        print("✓ Diagnóstico básico: Factible")
        return True
    
    @staticmethod
    def relajar_capacidades(capacidades, factor=1.2):
        """Relaja capacidades para hacer el problema factible"""
        print(f"Relajando capacidades (factor: {factor})...")
        nuevas_capacidades = {}
        for patio_id, capacidad in capacidades.items():
            nuevas_capacidades[patio_id] = int(capacidad * factor)
        return nuevas_capacidades
    
    @staticmethod
    def ajustar_balance_carga(total_rutas, total_patios, factor_min=0.5, factor_max=2.0):
        """Ajusta los límites de balance de carga"""
        carga_promedio = total_rutas / total_patios
        min_rutas = max(1, int(carga_promedio * factor_min))
        max_rutas = int(carga_promedio * factor_max)
        return min_rutas, max_rutas

class OptimizadorAsignacion:
    def __init__(self):
        self.config = Config()
        self.diagnostico = DiagnosticoInfactibilidad()
        
    def cargar_datos_optimizacion(self):
        """Carga todos los datos necesarios para la optimización"""
        print("Cargando datos para optimización...")
        
        try:
            # Cargar matriz de distancias
            with open(os.path.join(Config.DATA_PROCESSED, "matriz_distancias_hibrida.json"), 'r') as f:
                self.matriz_distancias = json.load(f)
            
            # Cargar matriz de tiempos
            with open(os.path.join(Config.DATA_PROCESSED, "matriz_tiempos_hibrida.json"), 'r') as f:
                self.matriz_tiempos = json.load(f)
            
            # Cargar capacidades
            with open(os.path.join(Config.DATA_PROCESSED, "capacidades_patios.json"), 'r') as f:
                capacidades_data = json.load(f)
                self.capacidades = capacidades_data["capacidades"]
                self.cap_total = capacidades_data["cap_total"]
            
            # Cargar datos geoespaciales para metadata
            self.patios = gpd.read_file(os.path.join(Config.DATA_PROCESSED, "patios_bk.geojson"))
            self.rutas = gpd.read_file(os.path.join(Config.DATA_PROCESSED, "rutas_bk.geojson"))
            
            print("✓ Datos de optimización cargados exitosamente")
            self._mostrar_estadisticas_carga()
            return True
            
        except Exception as e:
            print(f"❌ Error cargando datos: {e}")
            return False
    
    def _mostrar_estadisticas_carga(self):
        """Muestra estadísticas de los datos cargados"""
        print(f"\n=== ESTADÍSTICAS DE CARGA ===")
        print(f"Patios disponibles: {len(self.matriz_distancias)}")
        
        # Contar rutas únicas
        todas_rutas = set()
        for patio_id, rutas in self.matriz_distancias.items():
            todas_rutas.update(rutas.keys())
        
        self.rutas_ids = list(todas_rutas)
        print(f"Rutas disponibles: {len(self.rutas_ids)}")
        print(f"Capacidad total del sistema: {self.cap_total} rutas")
        print(f"Relación rutas/capacidad: {len(self.rutas_ids)}/{self.cap_total} = {len(self.rutas_ids)/self.cap_total:.2f}")
        
        # Mostrar distribución de rutas por patio
        print(f"\nDistribución de rutas por patio:")
        for patio_id in list(self.matriz_distancias.keys())[:5]:  # Mostrar primeros 5
            rutas_patio = len(self.matriz_distancias[patio_id])
            capacidad = self.capacidades.get(patio_id, Config.CAPACIDAD_PATIO_DEFAULT)
            print(f"  Patio {patio_id}: {rutas_patio} rutas posibles, capacidad: {capacidad}")
    
    def verificar_factibilidad(self):
        """Verifica si el problema es factible antes de optimizar"""
        return self.diagnostico.verificar_factibilidad_basica(
            self.matriz_distancias, self.capacidades, self.rutas_ids
        )
    
    def crear_modelo_ple(self, objetivo='distancia', balance_carga=True, relajar_restricciones=False):
        """
        Crea el modelo de Programación Lineal Entera con opciones de relajación
        """
        print(f"\nCreando modelo de optimización PLE...")
        print(f"Objetivo: Minimizar {objetivo}")
        print(f"Balance de carga: {balance_carga}")
        print(f"Relajar restricciones: {relajar_restricciones}")
        
        # Preparar conjuntos
        self.patios_ids = list(self.matriz_distancias.keys())
        
        # Ajustar capacidades si es necesario
        capacidades_ajustadas = self.capacidades.copy()
        if relajar_restricciones:
            capacidades_ajustadas = self.diagnostico.relajar_capacidades(self.capacidades, factor=1.5)
        
        # Crear problema de optimización
        if objetivo == 'distancia':
            self.prob = pulp.LpProblem("Asignacion_Optima_Patios_Rutas_Distancia", pulp.LpMinimize)
        else:
            self.prob = pulp.LpProblem("Asignacion_Optima_Patios_Rutas_Tiempo", pulp.LpMinimize)
        
        # Variables de decisión binarias
        self.x = pulp.LpVariable.dicts(
            "Asignacion",
            [(i, j) for i in self.patios_ids for j in self.rutas_ids if j in self.matriz_distancias[i]],
            cat=pulp.LpBinary
        )
        
        # FUNCIÓN OBJETIVO
        if objetivo == 'distancia':
            self.prob += pulp.lpSum(
                self.x[(i, j)] * self.matriz_distancias[i][j] 
                for i in self.patios_ids 
                for j in self.rutas_ids 
                if j in self.matriz_distancias[i]
            ), "Minimizar_Distancia_Total"
        else:
            self.prob += pulp.lpSum(
                self.x[(i, j)] * self.matriz_tiempos[i][j] 
                for i in self.patios_ids 
                for j in self.rutas_ids 
                if j in self.matriz_tiempos[i]
            ), "Minimizar_Tiempo_Total"
        
        # RESTRICCIONES
        
        # 1. Cada ruta asignada a exactamente un patio
        rutas_con_restriccion = 0
        for j in self.rutas_ids:
            patios_validos = [i for i in self.patios_ids if j in self.matriz_distancias[i]]
            if patios_validos:
                self.prob += pulp.lpSum(self.x[(i, j)] for i in patios_validos) == 1, f"Ruta_Asignada_{j}"
                rutas_con_restriccion += 1
        
        print(f"  - Restricciones de ruta: {rutas_con_restriccion}")
        
        # 2. No superar capacidad de cada patio (con capacidades ajustadas)
        patios_con_restriccion = 0
        for i in self.patios_ids:
            capacidad = capacidades_ajustadas.get(i, Config.CAPACIDAD_PATIO_DEFAULT)
            rutas_validas = [j for j in self.rutas_ids if j in self.matriz_distancias[i]]
            if rutas_validas:
                self.prob += pulp.lpSum(self.x[(i, j)] for j in rutas_validas) <= capacidad, f"Capacidad_Patio_{i}"
                patios_con_restriccion += 1
        
        print(f"  - Restricciones de capacidad: {patios_con_restriccion}")
        
        # 3. Balance de carga (opcional, con ajustes si se relajan)
        if balance_carga:
            total_rutas = len(self.rutas_ids)
            total_patios = len(self.patios_ids)
            if total_patios > 0:
                if relajar_restricciones:
                    min_rutas, max_rutas = self.diagnostico.ajustar_balance_carga(total_rutas, total_patios, 0.3, 3.0)
                else:
                    min_rutas, max_rutas = self.diagnostico.ajustar_balance_carga(total_rutas, total_patios, 0.75, 1.25)
                
                balance_count = 0
                for i in self.patios_ids:
                    rutas_validas = [j for j in self.rutas_ids if j in self.matriz_distancias[i]]
                    if rutas_validas and len(rutas_validas) >= min_rutas:
                        self.prob += pulp.lpSum(self.x[(i, j)] for j in rutas_validas) >= min_rutas, f"Min_Rutas_Patio_{i}"
                        self.prob += pulp.lpSum(self.x[(i, j)] for j in rutas_validas) <= max_rutas, f"Max_Rutas_Patio_{i}"
                        balance_count += 1
                
                print(f"  - Restricciones de balance: {balance_count}")
                print(f"    (Límites: {min_rutas}-{max_rutas} rutas por patio)")
        
        print("✓ Modelo PLE creado exitosamente")
        print(f"  - Variables: {len(self.x)}")
        print(f"  - Restricciones totales: {len(self.prob.constraints)}")
        
        return True
    
    def resolver_modelo(self, tiempo_limite=300, mostrar_progreso=True):
        """Resuelve el modelo de optimización con manejo de infactibilidad"""
        print(f"\nResolviendo modelo de optimización...")
        print(f"Tiempo límite: {tiempo_limite} segundos")
        
        # Configurar solver
        solver = pulp.PULP_CBC_CMD(
            msg=1 if mostrar_progreso else 0,
            timeLimit=tiempo_limite,
            gapRel=0.02  # 2% de gap de optimalidad
        )
        
        # Resolver
        start_time = datetime.now()
        self.prob.solve(solver)
        solve_time = (datetime.now() - start_time).total_seconds()
        
        # Mostrar resultados
        print(f"\n=== RESULTADOS DE OPTIMIZACIÓN ===")
        print(f"Estado: {pulp.LpStatus[self.prob.status]}")
        print(f"Tiempo de solución: {solve_time:.2f} segundos")
        
        if self.prob.status == pulp.LpStatusOptimal:
            valor_objetivo = pulp.value(self.prob.objective)
            unidad = "km" if "Distancia" in str(self.prob.objective) else "minutos"
            print(f"Solución ÓPTIMA encontrada: {valor_objetivo:.2f} {unidad}")
            return True
            
        elif self.prob.status == pulp.LpStatusInfeasible:
            print("❌ PROBLEMA INFACTIBLE")
            self._diagnosticar_infactibilidad()
            return False
            
        else:
            print(f"Estado no óptimo: {pulp.LpStatus[self.prob.status]}")
            return False
    
    def _diagnosticar_infactibilidad(self):
        """Diagnostica las causas de infactibilidad"""
        print("\n🔍 DIAGNOSTICANDO CAUSAS DE INFACTIBILIDAD...")
        
        # 1. Verificar capacidad total vs rutas
        total_rutas = len(self.rutas_ids)
        capacidad_total = sum(self.capacidades.values())
        print(f"1. Capacidad vs Rutas: {total_rutas} rutas / {capacidad_total} capacidad")
        
        if total_rutas > capacidad_total:
            print(f"   ❌ CAPACIDAD INSUFICIENTE: Necesitas {total_rutas - capacidad_total} más de capacidad")
            print(f"   💡 SOLUCIÓN: Aumentar capacidades o reducir número de rutas")
        
        # 2. Verificar rutas sin patio posible
        rutas_sin_patio = []
        for ruta_id in self.rutas_ids:
            patios_posibles = [p for p in self.patios_ids if ruta_id in self.matriz_distancias[p]]
            if not patios_posibles:
                rutas_sin_patio.append(ruta_id)
        
        if rutas_sin_patio:
            print(f"2. Rutas sin patio posible: {len(rutas_sin_patio)} rutas")
            print(f"   ❌ RUTAS AISLADAS: {rutas_sin_patio[:3]}...")
            print(f"   💡 SOLUCIÓN: Revisar matriz de distancias o añadir patios")
        
        # 3. Verificar patios con capacidad insuficiente para sus rutas posibles
        print(f"3. Análisis patio por patio:")
        for patio_id in self.patios_ids[:10]:  # Mostrar primeros 10
            capacidad = self.capacidades.get(patio_id, Config.CAPACIDAD_PATIO_DEFAULT)
            rutas_posibles = list(self.matriz_distancias[patio_id].keys())
            if len(rutas_posibles) < capacidad:
                print(f"   Patio {patio_id}: Capacidad ({capacidad}) > Rutas posibles ({len(rutas_posibles)})")
        
        print(f"\n💡 RECOMENDACIONES:")
        print(f"   - Ejecutar con relajar_restricciones=True")
        print(f"   - Revisar capacidades en capacidades_patios.json")
        print(f"   - Verificar matriz_distancias_hibrida.json")
    
    def resolver_con_relajacion(self, max_intentos=3):
        """Intenta resolver el problema relajando restricciones gradualmente"""
        print("\n=== INTENTANDO SOLUCIÓN CON RELAJACIÓN ===")
        
        estrategias = [
            {'balance_carga': True, 'relajar_restricciones': False, 'nombre': 'Estricto'},
            {'balance_carga': True, 'relajar_restricciones': True, 'nombre': 'Relajado 1'},
            {'balance_carga': False, 'relajar_restricciones': True, 'nombre': 'Relajado 2'},
        ]
        
        for i, estrategia in enumerate(estrategias):
            print(f"\n--- Intento {i+1}: {estrategia['nombre']} ---")
            
            self.crear_modelo_ple(
                objetivo='distancia',
                balance_carga=estrategia['balance_carga'],
                relajar_restricciones=estrategia['relajar_restricciones']
            )
            
            if self.resolver_modelo(tiempo_limite=180):
                # Éxito - extraer y guardar resultados
                resultados = self.extraer_resultados()
                if resultados is not None:
                    # Guardar con nombre diferente
                    resultados.to_csv(
                        os.path.join(Config.DATA_RESULTS, f"asignaciones_{estrategia['nombre'].lower().replace(' ', '_')}.csv"),
                        index=False
                    )
                    print(f"✓ Solución encontrada con estrategia: {estrategia['nombre']}")
                    return True
            
            if i < len(estrategias) - 1:
                print("⏭️  Probando siguiente estrategia...")
        
        print("❌ No se pudo encontrar solución factible")
        return False
    
    def extraer_resultados(self):
        """Extrae y formatea los resultados de la optimización"""
        if self.prob.status != pulp.LpStatusOptimal:
            return None
        
        print("\nExtrayendo resultados de la optimización...")
        
        # Recolectar asignaciones óptimas
        asignaciones = []
        for i in self.patios_ids:
            for j in self.rutas_ids:
                if (i, j) in self.x and pulp.value(self.x[(i, j)]) == 1:
                    asignaciones.append({
                        'patio_id': i,
                        'ruta_id': j,
                        'distancia_km': self.matriz_distancias[i][j],
                        'tiempo_minutos': self.matriz_tiempos[i][j],
                        'patio_nombre': self._obtener_nombre_patio(i),
                        'ruta_nombre': self._obtener_nombre_ruta(j)
                    })
        
        self.resultados = pd.DataFrame(asignaciones)
        
        if len(self.resultados) > 0:
            self._calcular_metricas()
            print(f"✓ Resultados extraídos: {len(self.resultados)} asignaciones")
            return self.resultados
        else:
            print("❌ No se pudieron extraer resultados")
            return None
    
    def _obtener_nombre_patio(self, patio_id):
        """Obtiene el nombre del patio desde los datos geoespaciales"""
        try:
            patio = self.patios[self.patios['objectid'] == int(patio_id)]
            if not patio.empty:
                return patio.iloc[0].get('nombre', f'Patio {patio_id}')
        except:
            pass
        return f'Patio {patio_id}'
    
    def _obtener_nombre_ruta(self, ruta_id):
        """Obtiene el nombre de la ruta desde los datos geoespaciales"""
        try:
            ruta = self.rutas[self.rutas['route_id'] == ruta_id]
            if not ruta.empty:
                return ruta.iloc[0].get('ruta', f'Ruta {ruta_id}')
        except:
            pass
        return f'Ruta {ruta_id}'
    
    def _calcular_metricas(self):
        """Calcula métricas de desempeño de la solución"""
        print("\nCalculando métricas de la solución...")
        
        # Métricas por patio
        self.metricas_patios = self.resultados.groupby('patio_id').agg({
            'ruta_id': 'count',
            'distancia_km': ['sum', 'mean', 'std'],
            'tiempo_minutos': ['sum', 'mean', 'std']
        }).round(2)
        
        # Métricas generales
        self.distancia_total = self.resultados['distancia_km'].sum()
        self.tiempo_total = self.resultados['tiempo_minutos'].sum()
        self.distancia_promedio = self.resultados['distancia_km'].mean()
        self.tiempo_promedio = self.resultados['tiempo_minutos'].mean()
        
        print(f"\n=== MÉTRICAS DE LA SOLUCIÓN ===")
        print(f"Distancia total: {self.distancia_total:.2f} km")
        print(f"Tiempo total: {self.tiempo_total:.2f} minutos")
        print(f"Distancia promedio: {self.distancia_promedio:.2f} km")
        print(f"Tiempo promedio: {self.tiempo_promedio:.2f} minutos")
        print(f"Asignaciones: {len(self.resultados)}")

def main():
    """Función principal de optimización con manejo robusto"""
    print("=== OPTIMIZACIÓN DE ASIGNACIÓN PATIO-RUTA ===")
    
    optimizador = OptimizadorAsignacion()
    
    # Cargar datos
    if not optimizador.cargar_datos_optimizacion():
        return
    
    # Verificar factibilidad primero
    if not optimizador.verificar_factibilidad():
        print("\n⚠️  Problema potencialmente infactible. Intentando con relajación...")
        optimizador.resolver_con_relajacion()
    else:
        # Intentar solución normal primero
        optimizador.crear_modelo_ple(
            objetivo='distancia',
            balance_carga=True,
            relajar_restricciones=False
        )
        
        if not optimizador.resolver_modelo():
            # Si falla, intentar con relajación
            print("\n⚠️  Solución estricta falló. Intentando con relajación...")
            optimizador.resolver_con_relajacion()
    
    print("\n=== PROCESO DE OPTIMIZACIÓN COMPLETADO ===")

if __name__ == "__main__":
    main()