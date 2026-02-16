# add_elevation_data_resumable.py
import neo4j
import requests
import json
import time
from tqdm import tqdm

# --- CONFIGURE YOUR NEO4J CONNECTION ---
NEO4J_URI = "bolt://100.53.46.182:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "descriptions-propose-overlays"
# -----------------------------------------

driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
ELEVATION_API_URL = "https://api.open-meteo.com/v1/elevation"
BATCH_SIZE = 100

def fetch_and_update_elevation():
    """
    Fetches one batch, gets data from Open-Meteo, and updates Neo4j.
    Returns the number of nodes updated (e.g., 100).
    Returns 0 if all nodes are done.
    Returns -1 on a critical error.
    """
    with driver.session(database="neo4j") as session:
        nodes_to_process = session.execute_read(get_nodes_without_elevation, BATCH_SIZE)
        
        if not nodes_to_process:
            return 0 # All done

        latitudes = [str(record["lat"]) for record in nodes_to_process]
        longitudes = [str(record["lon"]) for record in nodes_to_process]
        
        params = {"latitude": ",".join(latitudes), "longitude": ",".join(longitudes)}
        
        try:
            response = requests.get(ELEVATION_API_URL, params=params, timeout=45)
            response.raise_for_status()
            
            elevation_results = response.json()['elevation']
            
            updates = []
            for i, record in enumerate(nodes_to_process):
                updates.append({"osmid": record["osmid"], "elevation": elevation_results[i]})
            
            session.execute_write(update_nodes_with_elevation, updates)
            return len(updates)

        except requests.exceptions.RequestException as e:
            print(f"\n✗ An API error occurred: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"   Status Code: {e.response.status_code}")
                if e.response.status_code == 429:
                    print("   This is a rate-limit error. Please wait and re-run the script later.")
            return -1

def get_total_nodes_without_elevation(tx):
    """Counts nodes missing the elevation property."""
    query = "MATCH (i:Intersection) WHERE i.elevation IS NULL RETURN count(i) AS count"
    result = tx.run(query).single()
    return result["count"] if result else 0

def get_nodes_without_elevation(tx, batch_size):
    """Gets a batch of nodes missing the elevation property."""
    query = """
    MATCH (i:Intersection)
    WHERE i.elevation IS NULL
    RETURN i.osmid AS osmid, i.location.latitude AS lat, i.location.longitude AS lon
    LIMIT $batch_size
    """
    results = tx.run(query, batch_size=batch_size)
    return [record.data() for record in results]

def update_nodes_with_elevation(tx, updates):
    """Updates nodes with fetched elevation data."""
    query = """
    UNWIND $updates AS update
    MATCH (i:Intersection {osmid: update.osmid})
    SET i.elevation = toFloat(update.elevation)
    """
    tx.run(query, updates=updates)

# Main execution block
if __name__ == "__main__":
    print("Starting/resuming elevation data enrichment for Campina Grande...")
    
    with driver.session(database="neo4j") as session:
        total_missing = session.execute_read(get_total_nodes_without_elevation)

    if total_missing == 0:
        print("✓ All nodes already have elevation data!")
    else:
        print(f"Found {total_missing} nodes that need elevation data.")
        with tqdm(total=total_missing, desc="Updating nodes", unit="node") as pbar:
            while True:
                nodes_updated = fetch_and_update_elevation()
                
                if nodes_updated == 0: # All done
                    # Ensure the progress bar completes fully
                    pbar.update(pbar.total - pbar.n)
                    break
                
                if nodes_updated < 0: # Error occurred
                    print("Stopping due to an error. You can safely re-run this script later to continue.")
                    break
                
                pbar.update(nodes_updated)
                
                # A much more polite delay to avoid rate limits
                time.sleep(10) 
    
    print("✓ Elevation enrichment script finished.")
    driver.close()