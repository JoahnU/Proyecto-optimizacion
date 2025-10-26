# scripts/2d_matriz_distancias_hibrida.py
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from scipy.spatial.distance import cdist
import networkx as nx
from sklearn.neighbors import NearestNeighbors
import os
from config import Config

class MatrizDistanciasHibrida:
    def __init__(self):
        self.config = Config()
        
    def crear_grafo_vial_aproximado(self, patios_gdf, paraderos_gdf):
        """Crea un grafo vial aproximado usando datos de OpenStreetMap"""
        print("Creando grafo vial aproximado...")
        
        # Puntos de referencia para Bosa/Kennedy (puedes ampliar esta lista)
        puntos_viales = {
            'Bosa_Centro': (4.6333, -74.1895),
            'Kennedy_Centro': (4.6500, -74.1500),
            'Portal_Américas': (4.6092, -74.1469),
            'Portal_Bosa': (4.6278, -74.2028),
            'Av_Boyacá': (4.6200, -74.1300),
            'Av_Ciudad_de_Cali': (4.6400, -74.1700)
        }
        
        # Crear grafo
        G = nx.Graph()
        
        # Añadir nodos
        for nombre, coords in puntos_viales.items():
            G.add_node(nombre, pos=coords)
        
        # Añadir aristas con distancias reales aproximadas
        conexiones = [
            ('Bosa_Centro', 'Portal_Bosa', 3.2),
            ('Bosa_Centro', 'Av_Ciudad_de_Cali', 4.1),
            ('Kennedy_Centro', 'Portal_Américas', 2.8),
            ('Kennedy_Centro', 'Av_Boyacá', 3.5),
            ('Portal_Américas', 'Av_Boyacá', 2.1),
            ('Av_Ciudad_de_Cali', 'Av_Boyacá', 5.2)
        ]
        
        for origen, destino, distancia in conexiones:
            G.add_edge(origen, destino, weight=distancia)
        
        return G, puntos_viales
    
    def calcular_distancias_hibridas(self, patios_gdf, rutas_gdf, primeros_paraderos):
        """Calcula distancias usando método híbrido"""
        print("Calculando matriz híbrida de distancias...")
        
        # Crear grafo vial aproximado
        G, puntos_viales = self.crear_grafo_vial_aproximado(patios_gdf, primeros_paraderos)
        
        matriz_distancias = {}
        matriz_tiempos = {}
        
        # Convertir a coordenadas para cálculos
        coords_patios = []
        for patio in patios_gdf.itertuples():
            coords_patios.append((patio.geometry.y, patio.geometry.x))
        
        coords_paraderos = []
        ruta_ids = []
        for ruta_id, paradero in primeros_paraderos.items():
            coords_paraderos.append((paradero['geometry'].y, paradero['geometry'].x))
            ruta_ids.append(ruta_id)
        
        # Matriz de distancias lineales
        dist_lineales = cdist(coords_patios, coords_paraderos, 
                            lambda u, v: geodesic(u, v).kilometers)
        
        # Aplicar factores de corrección basados en zona
        for i, patio in enumerate(patios_gdf.itertuples()):
            patio_id = patio.objectid
            matriz_distancias[patio_id] = {}
            matriz_tiempos[patio_id] = {}
            
            # Determinar zona del patio
            zona_patio = self._clasificar_zona((patio.geometry.y, patio.geometry.x))
            
            for j, ruta_id in enumerate(ruta_ids):
                paradero_coords = (coords_paraderos[j][0], coords_paraderos[j][1])
                zona_paradero = self._clasificar_zona(paradero_coords)
                
                # Distancia lineal base
                dist_base = dist_lineales[i][j]
                
                # Aplicar factores de corrección
                factor_correccion = self._calcular_factor_correccion(zona_patio, zona_paradero, dist_base)
                distancia_corregida = dist_base * factor_correccion
                
                # Calcular tiempo considerando congestión
                tiempo_minutos = self._calcular_tiempo_viaje(distancia_corregida, zona_patio, zona_paradero)
                
                matriz_distancias[patio_id][ruta_id] = round(distancia_corregida, 2)
                matriz_tiempos[patio_id][ruta_id] = round(tiempo_minutos, 2)
        
        return matriz_distancias, matriz_tiempos
    
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