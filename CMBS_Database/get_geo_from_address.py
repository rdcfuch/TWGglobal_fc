import pandas as pd
import geopandas as gpd

# Perform geocoding
gdf = gpd.tools.geocode("Champ de Mars, 5 Avenue Anatole France, 75007 Paris, France")

# Print the coordinates
gdf.geometry.iloc[0].x, gdf.geometry.iloc[0].y