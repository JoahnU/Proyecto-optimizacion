import pandas as pd
import geopandas as gpd
from geopy.distance import geodesic

# === 1. Cargar archivos GTFS ===
# Cargar los archivos GTFS
routes = pd.read_csv("C:/Users/prestamour/Downloads/algo/data/raw/gtfs/routes.txt")         # Información de las rutas
trips = pd.read_csv("C:/Users/prestamour/Downloads/algo/data/raw/gtfs/trips.txt")           # Información de los viajes
stop_times = pd.read_csv("C:/Users/prestamour/Downloads/algo/data/raw/gtfs/stop_times.txt") # Información de las paradas por viaje
stops = pd.read_csv("C:/Users/prestamour/Downloads/algo/data/raw/gtfs/stops.txt")           # Información de las paradas (con coordenadas)

# === 2. Cargar patios ===
# Cargar el archivo geojson de los patios SITP
patios = gpd.read_file("C:/Users/prestamour/Downloads/algo/data/processed/patios_bk.geojson")
# Calcular el centroide de cada patio (si son polígonos)
patios["coords"] = patios.geometry.centroid.apply(lambda g: (g.y, g.x))

# === 3. Obtener la primera parada de cada ruta ===
# Relacionar stop_times con trips, stops y routes
merged = (stop_times
          .merge(trips, on="trip_id")       # Unir con trips para identificar la ruta
          .merge(stops, on="stop_id")       # Unir con stops para obtener las coordenadas
          .merge(routes, on="route_id"))    # Unir con routes para identificar la ruta

# Ordenar por la secuencia de las paradas para cada ruta
first_stops = (merged.sort_values("stop_sequence")
               .groupby("route_id")
               .first()
               .reset_index())

# Crear una columna con las coordenadas de la primera parada
first_stops["coords"] = list(zip(first_stops["stop_lat"], first_stops["stop_lon"]))

# === 4. Filtrar rutas específicas ===
# Aquí puedes definir una lista con las rutas específicas que te interesa calcular"
rutas_especificas = ["L807", "P7", "F424", "K333", "H712", "D208", "F417", "F402", "F425", "F414", "G414", "F512", "G512", "G524", "C157", "G528", "F407", "G514", "H521", "F409", "H603", "Z8", "C137", "C156", "T30B", "B902", "F511", "G511", "786", "C147", "TC14", "B918", "H615", "G525", "F423"]  # Lista de rutas que deseas analizar

# Filtrar el DataFrame para obtener solo las rutas que te interesan
first_stops_filtradas = first_stops[first_stops["route_short_name"].isin(rutas_especificas)]

# === 5. Calcular el patio más cercano para las rutas filtradas ===
results = []

# Iterar sobre cada primera parada de ruta filtrada
for idx, row in first_stops_filtradas.iterrows():
    route_id = row["route_short_name"]
    first_stop_coords = row["coords"]

    # Calcular las distancias a todos los patios
    patios["dist_km"] = patios["coords"].apply(lambda c: geodesic(first_stop_coords, c).km)



    # Encontrar el patio más cercano
    closest_patio = patios.loc[patios["dist_km"].idxmin()]
    results.append({
        "route_id": route_id,
        "closest_patio": closest_patio.get("nom_patio", closest_patio.get("id", "Desconocido")),
        "distance_km": closest_patio["dist_km"]
    })

# === 6. Crear un DataFrame con los resultados ===
dist_matrix = pd.DataFrame(results)

# === 7. Eliminar duplicados con mayor distancia ===
# Ordenar por distancia de forma ascendente
dist_matrix = dist_matrix.sort_values("distance_km")

# Eliminar duplicados basándose en `route_id`, manteniendo el de menor distancia
dist_matrix_unique = dist_matrix.drop_duplicates(subset="route_id", keep="first")

# Mostrar el DataFrame sin duplicados
print(dist_matrix_unique)

# Guardar los resultados en un archivo CSV
dist_matrix_unique.to_csv("distancias_ultima_parada_patio.csv", index=False)

