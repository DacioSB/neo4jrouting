import neo4j
import osmnx as ox
import math
from tqdm.auto import tqdm
import pandas as pd

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "your-secret-password"

print("Connecting to Neo4j database...")
driver = neo4j.GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
print("Connected.")
print("Downloading street network data for San Mateo, CA...")
G = ox.graph_from_place("San Mateo, CA, USA", network_type="drive")
print("Data downloaded.")
print("Converting graph to GeoDataFrames...")
gdf_nodes, gdf_relationships = ox.graph_to_gdfs(G)
gdf_nodes.reset_index(inplace=True)
gdf_relationships.reset_index(inplace=True)
print("Conversion complete.")

NODE_COLUMNS = ['osmid', 'y', 'x', 'street_count']
REL_COLUMNS = ['u', 'v', 'osmid', 'oneway', 'name', 'highway', 'maxspeed', 'length']

def insert_data(tx, query, rows, columns_to_keep, batch_size=10000, label="rows"):
    """
    This function is executed within a managed Neo4j transaction.
    It iterates over the provided DataFrame `rows` and executes the `query` in batches.
    """
    if rows.empty:
        print(f"No {label} to insert. Skipping.")
        return 0
    total_inserted = 0
    batch = math.ceil(len(rows) / batch_size)

    for batch in tqdm(range(batch), desc=f"Inserting {label}", unit="batch"):
        start_idx = batch * batch_size
        end_idx = min((batch + 1) * batch_size, len(rows))
        batch_df = rows.iloc[start_idx:end_idx]
        batch_df_subset = batch_df[columns_to_keep]
        batch_rows = batch_df_subset.to_dict("records")
        result = tx.run(query, parameters={"rows": batch_rows}).single()
        if result and result["total"]:
            total_inserted += result["total"]
        print(f"Successfully inserted a total of {total_inserted} {label}.")

    return total_inserted

# Cypher query for nodes
node_query = '''
    UNWIND $rows AS row
    WITH row WHERE row.osmid IS NOT NULL
    MERGE (i:Intersection {osmid: row.osmid})
    SET i.location = point({latitude: row.y, longitude: row.x }),
        i.street_count = toInteger(row.street_count)
    RETURN COUNT(*) as total
'''

# Cypher query for relationships
rels_query = '''
    UNWIND $rows AS road
    MATCH (u:Intersection {osmid: road.u})
    MATCH (v:Intersection {osmid: road.v})
    MERGE (u)-[r:ROAD_SEGMENT {osmid: road.osmid}]->(v)
    SET r.oneway = road.oneway,
        r.name = road.name,
        r.highway = road.highway,
        r.max_speed = road.maxspeed,
        r.length = toFloat(road.length)
    RETURN COUNT(*) AS total
'''

def create_constraints(tx):
    print("Creating uniqueness constraint for :Intersection(osmid)...")
    tx.run("CREATE CONSTRAINT IF NOT EXISTS FOR (i:Intersection) REQUIRE i.osmid IS UNIQUE")
    print("Constraint created or already exists.")

with driver.session() as session:
    session.execute_write(create_constraints)
    print("Inserting nodes into the database...")
    total_nodes = session.execute_write(insert_data, node_query, gdf_nodes, NODE_COLUMNS, label="nodes")
    print(f"Total nodes inserted: {total_nodes}")
    print("Inserting relationships into the database...")
    total_rels = session.execute_write(insert_data, rels_query, gdf_relationships, REL_COLUMNS, label="relationships")
    print(f"Total relationships inserted: {total_rels}")

print("Data insertion complete.")
driver.close()