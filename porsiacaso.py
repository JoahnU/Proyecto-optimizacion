    def crear_matriz_distancias(self):
        """Crea matriz de distancias entre patios y primeros paraderos"""
        print("Calculando matriz de distancias...")
        
        # Proyectar a coordenadas planas para cálculos precisos
        patios_proj = self.patios_bk.to_crs(Config.CRS_PROYECCION)
        paraderos_proj = self.primeros_paraderos_gtfs.to_crs(Config.CRS_PROYECCION)
        
        # Para cada ruta, encontrar el primer paradero más cercano
        self.distancias = {}
        
        for _, patio in patios_proj.iterrows():
            patio_id = patio['objectid']  # Ajustar según estructura real
            self.distancias[patio_id] = {}
            
            for _, ruta in self.rutas_bk.iterrows():
                route_id = ruta['route_id']  # Ajustar según estructura real
                
                # Filtrar primeros paraderos para esta ruta
                paraderos_ruta = paraderos_proj[
                    paraderos_proj['route_id'] == route_id
                ]
                
                if len(paraderos_ruta) > 0:
                    # Calcular distancia al paradero más cercano
                    distancias = paraderos_ruta.geometry.distance(patio.geometry)
                    distancia_min = distancias.min() / 1000  # Convertir a km
                    
                    self.distancias[patio_id][route_id] = distancia_min
                else:
                    # Si no hay datos GTFS, usar aproximación
                    distancia_aproximada = self._calcular_distancia_aproximada(patio, ruta)
                    self.distancias[patio_id][route_id] = distancia_aproximada
        
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