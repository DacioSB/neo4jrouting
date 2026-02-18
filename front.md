CREATE FULLTEXT INDEX addressSearchIndex IF NOT EXISTS
FOR (n:Address)
ON EACH [n.full_address];```
*(Note: I've given it a more descriptive name, `addressSearchIndex`)*

---

### Part 1: The Vue.js + Vite Version

We will use Vue 3 with the Composition API (`<script setup>`), which is the modern and recommended standard.

#### Step 1: Create a New Vue Project with Vite

Open your terminal and run the following command. It will guide you through an interactive setup.

```bash
# Start the creation process
npm create vite@latest

# You will be prompted:
# ✔ Project name: ... campina-grande-routing-vue
# ✔ Select a framework: › Vue
# ✔ Select a variant: › JavaScript (or TypeScript if you prefer)

# Now, navigate into the directory and install dependencies
cd campina-grande-routing-vue
npm install

npm install leaflet vue-leaflet neo4j-driver vue-select

// src/neo4j.js
```javascript
import neo4j from 'neo4j-driver';

const URI = "<YOUR_AURA_INSTANCE_URI>"; // e.g., "neo4j+s://xxxxxx.databases.neo4j.io"
const USER = "neo4j";
const PASSWORD = "<YOUR_AURA_INSTANCE_PASSWORD>";

let driver;
try {
  driver = neo4j.driver(URI, neo4j.auth.basic(USER, PASSWORD));
  console.log("Neo4j driver initialized for Vue app");
} catch (error) {
  console.error("Failed to create Neo4j driver:", error);
}

export default driver;
```

#### Step 2: Create the Main App Component
```javascript
<template>
  <div class="app-container">
    <header class="header">
      <h1>Campina Grande Smart Routing (Vue)</h1>
      <div class="controls">
        <div class="input-group">
          <label>Start</label>
          <v-select
            class="address-select"
            :options="startOptions"
            @search="debounceSearchStart"
            v-model="startPoint"
            placeholder="Search for a starting address..."
            :filterable="false"
          />
        </div>
        <div class="input-group">
          <label>End</label>
          <v-select
            class="address-select"
            :options="endOptions"
            @search="debounceSearchEnd"
            v-model="endPoint"
            placeholder="Search for a destination..."
            :filterable="false"
          />
        </div>
        <div class="input-group">
          <label>Route Type</label>
          <select v-model="routeType">
            <option value="length">Shortest</option>
            <option value="current_travel_time">Fastest (with Traffic)</option>
            <option value="eco_cost">Greenest</option>
          </select>
        </div>
        <button class="route-button" @click="calculateRoute" :disabled="!startPoint || !endPoint">
          Find Route
        </button>
      </div>
    </header>

    <l-map ref="map" :zoom="14" :center="centerOfCampinaGrande" style="height: 100%; width: 100%;">
      <l-tile-layer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="http://osm.org/copyright">OpenStreetMap</a> contributors'
      ></l-tile-layer>
      
      <l-marker v-if="startPoint" :lat-lng="[startPoint.location.y, startPoint.location.x]"></l-marker>
      <l-marker v-if="endPoint" :lat-lng="[endPoint.location.y, endPoint.location.x]"></l-marker>
      
      <l-polyline v-if="route.length > 0" :lat-lngs="route" color="blue" :weight="5"></l-polyline>
    </l-map>
  </div>
</template>

<script setup>
import { ref, watch } from 'vue';
import { LMap, LTileLayer, LPolyline, LMarker } from "@vue-leaflet/vue-leaflet";
import "leaflet/dist/leaflet.css";
import vSelect from "vue-select";
import "vue-select/dist/vue-select.css";
import L from 'leaflet';
import driver from './neo4j';

// --- STATE MANAGEMENT (Composition API) ---
const startPoint = ref(null);
const endPoint = ref(null);
const startOptions = ref([]);
const endOptions = ref([]);
const routeType = ref('length');
const route = ref([]);
const map = ref(null); // Reference to the map component
const centerOfCampinaGrande = [-7.2192, -35.8825];

// --- UTILITY ---
// Debounce function to prevent spamming the API on every keystroke
function debounce(fn, delay) {
  let timeoutId;
  return function(...args) {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => fn.apply(this, args), delay);
  };
}

// --- DATA FETCHING ---
const searchAddresses = async (search, loading, optionsRef) => {
  if (search.length < 3) return;
  loading(true);
  const session = driver.session();
  try {
    const result = await session.run(
      `CALL db.index.fulltext.queryNodes("addressSearchIndex", $search) YIELD node, score
       RETURN node.hash as value, node.full_address as label, node.location as location
       ORDER BY score DESC LIMIT 15`,
      { search: search + '~' }
    );
    optionsRef.value = result.records.map(record => ({
      value: record.get('value'),
      label: record.get('label'),
      location: record.get('location'),
    }));
  } finally {
    loading(false);
    session.close();
  }
};

const calculateRoute = async () => {
  if (!startPoint.value || !endPoint.value) return;
  route.value = [];
  const session = driver.session();
  try {
    // NOTE: GDS requires the projected graph to exist.
    // Ensure you have run the traffic simulation script that creates 'campina-grande-traffic-analysis'
    const result = await session.run(`
      MATCH (start_addr:Address {hash: $startHash})-[:NEAREST_INTERSECTION]->(start_node:Intersection)
      MATCH (end_addr:Address {hash: $endHash})-[:NEAREST_INTERSECTION]->(end_node:Intersection)
      CALL gds.shortestPath.dijkstra.stream('campina-grande-traffic-analysis', {
          sourceNode: start_node,
          targetNode: end_node,
          relationshipWeightProperty: $weightProp
      })
      YIELD nodeIds
      RETURN [nodeId IN nodeIds | [gds.util.asNode(nodeId).location.latitude, gds.util.asNode(nodeId).location.longitude]] AS routeCoords
    `, {
      startHash: startPoint.value.value,
      endHash: endPoint.value.value,
      weightProp: routeType.value
    });
    if (result.records.length > 0) {
      const routeCoords = result.records[0].get('routeCoords');
      if (routeCoords && routeCoords.length > 0) {
        route.value = routeCoords;
        const bounds = L.latLngBounds(routeCoords);
        map.value.leafletObject.fitBounds(bounds, { padding: [50, 50] });
      }
    }
  } catch (error) {
    console.error("Routing error:", error);
    alert("Could not calculate the route. The GDS graph projection might be missing or an address is outside the network.");
  } finally {
    session.close();
  }
};

const debounceSearchStart = debounce((search, loading) => searchAddresses(search, loading, startOptions), 500);
const debounceSearchEnd = debounce((search, loading) => searchAddresses(search, loading, endOptions), 500);
</script>

<style>
/* You can copy the CSS from the React version into src/style.css and import it */
/* Or copy it here */
body, html, #app {
  margin: 0; padding: 0; height: 100%; width: 100%;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
.app-container { display: flex; flex-direction: column; height: 100vh; }
.header { padding: 15px; background-color: #f8f9fa; border-bottom: 1px solid #dee2e6; z-index: 1000; }
.header h1 { margin: 0 0 15px 0; font-size: 1.5em; text-align: center; }
.controls { display: flex; flex-wrap: wrap; gap: 15px; align-items: flex-end; justify-content: center; }
.input-group { display: flex; flex-direction: column; flex: 1; min-width: 250px; }
.input-group label { font-weight: bold; margin-bottom: 5px; font-size: 0.9em; }
.address-select { width: 100%; background-color: white; }
.route-button { padding: 10px 20px; font-size: 1em; background-color: #007bff; color: white; border: none; border-radius: 5px; cursor: pointer; height: 40px; }
.route-button:disabled { background-color: #cccccc; cursor: not-allowed; }
.input-group select { height: 40px; border-radius: 5px; border: 1px solid #cccccc; padding: 0 10px; }
</style>
```