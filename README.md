# Neo4j Routing Web App

This project is a high-performance routing web application centered on Campina Grande, Brazil, built using the **Neo4j** graph database, **OpenStreetMap (OSM)** data, and **Leaflet.js** for interactive map rendering. It implements advanced graph algorithms to solve the weighted shortest path problem on a simplified road network.

## 🚀 Overview

Running path-finding algorithms on large geospatial datasets is a core strength of graph databases. This project transforms raw OSM data into a property graph model where intersections are nodes and road segments are relationships, allowing for real-time routing, traffic simulation, and elevation analysis.

### Key Features
*   **Simplified Road Topology:** Uses the `OSMNx` Python package to consolidate road segments into a clean intersection-to-intersection graph.
*   **Weighted Pathfinding:** Implements **Dijkstra’s Algorithm** and **A*** via the APOC library to find the most efficient routes based on distance and speed.
*   **Address Integration:** Maps OpenAddresses data to the nearest road intersections for precise "door-to-door" routing.
*   **Traffic Simulation:** Includes Cypher scripts to simulate dynamic traffic conditions and their impact on routing weights.
*   **Elevation Data:** Enhances the graph with elevation properties to analyze route difficulty.
*   **Modern Frontend:** A responsive **Vue.js** application that communicates directly with Neo4j using the JavaScript driver.

## 🛠️ Tech Stack

*   **Database:** Neo4j (AuraDB or Self-Managed).
*   **Data Sourcing:** OpenStreetMap (via Overpass API/OSMNx) and OpenAddresses.
*   **Backend Logic:** Cypher Query Language and APOC procedures.
*   **Frontend:** Vue.js, Leaflet.js, and Vite.
*   **Data Processing:** Python (Pandas, GeoPandas, OSMNx).

## 🏗️ Project Structure

The repository is organized into specialized modules for data processing and application logic:

*   **`python_scripts/`**: Contains scripts for filtering addresses (`filter_addresses.py`), processing GeoDataFrames (`geodf.py`), and adding elevation data (`add_elevation_data.py`).
*   **`base_cyphers/`**: Core database setup scripts, including specific queries for the Campina Grande region (`neoqueries_cg.cypher`).
*   **`routing_cyphers/`**: Logic for weighted routing and spatial indexing.
*   **`traffic_cyphers/`**: Scripts for Dijkstra implementation and traffic simulation.
*   **`frontend/campina-grande-routing-vue/`**: The Vue.js source code, including Neo4j driver utilities and map components.

## 🚦 Getting Started

### 1. Database Setup
Create a Neo4j AuraDB instance or a local database. Ensure the **APOC** library is installed, as it is required for Dijkstra and A* algorithms.

### 2. Data Import
Use the provided Python environment to fetch and simplify road data:
```bash
pip install neo4j osmnx geopandas
```
Run `python_scripts/geodf.py` to fetch the San Mateo or Campina Grande road network and import it into Neo4j using the optimized `UNWIND` and `MERGE` patterns.

### 3. Apply Constraints and Indices
Run the Cypher commands in `routing_cyphers/index.cypher` to ensure data integrity and performance:
*   Unique constraints on `osmid` for Intersections.
*   Point indices on the `location` property for fast spatial lookups.
*   Full-text search indices for address autocomplete.

### 4. Launch the Frontend
Navigate to the Vue project directory and start the development server:
```bash
cd frontend/campina-grande-routing-vue
npm install
npm run dev
```
Update `src/utils/neo4j.ts` with your database credentials to enable map interactions.

## 🔍 Core Cypher Logic

### Finding the Shortest Weighted Path
The application uses the `apoc.algo.dijkstra` procedure to calculate routes based on the `length` property of road segments:
```cypher
MATCH (source:Intersection {osmid: $startNode}), (target:Intersection {osmid: $endNode})
CALL apoc.algo.dijkstra(source, target, 'ROAD_SEGMENT', 'length')
YIELD path, weight
RETURN path, weight
```
This approach prioritizes exploring lower-cost routes first using a priority queue.

### Nearest Intersection Mapping
To connect arbitrary addresses to the road network, the project uses spatial distance queries:
```cypher
MATCH (a:Address), (i:Intersection)
WHERE point.distance(a.location, i.location) < 200
WITH a, i ORDER BY point.distance(a.location, i.location) ASC LIMIT 1
MERGE (a)-[r:NEAREST_INTERSECTION]->(i)
SET r.length = point.distance(a.location, i.location)
```
This batches the association of millions of features into a single, efficient graph structure.

## 📚 Resources
*   [William Lyon's Original Tutorial](https://neo4j.com/blog/build-routing-web-app-neo4j-openstreetmap-leaflet-js/)
*   [Neo4j Graph Data Science Documentation](https://neo4j.com/docs/graph-data-science/current/)
*   [Leaflet.js Documentation](https://leafletjs.com/)