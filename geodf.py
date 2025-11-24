import osmnx as ox
import matplotlib.pyplot as plt

# Step 1: Download San Mateo road network
print("Downloading street network data for San Mateo, CA...")
G = ox.graph_from_place("San Mateo, CA, USA", network_type="drive")
print("Graph downloaded.")

# Step 2: Convert to GeoDataFrames
print("Converting graph to GeoDataFrames...")
gdf_nodes, gdf_relationships = ox.graph_to_gdfs(G)
print("Converted.")

# Step 3: Plot and save
print("Plotting street network...")

fig, ax = plt.subplots(figsize=(10, 10))
gdf_relationships.plot(ax=ax, linewidth=0.5)

ax.set_title("San Mateo Road Network", fontsize=14)
ax.set_xlabel("Longitude")
ax.set_ylabel("Latitude")

output_filename = "san_mateo_network.png"
plt.savefig(output_filename, dpi=300, bbox_inches="tight")

plt.close(fig)

print(f"Image saved as {output_filename}")
