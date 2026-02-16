// Find the "greenest" path and format for web consumption
MATCH (start_addr:Address)-[:NEAREST_INTERSECTION]->(start:Intersection)
WHERE toUpper(start_addr.full_address) CONTAINS 'JOAO SUASSUNA'
WITH start, start_addr LIMIT 1

MATCH (end_addr:Address)-[:NEAREST_INTERSECTION]->(end:Intersection) 
WHERE toUpper(end_addr.full_address) CONTAINS 'AVENIDA FLORIANO PEIXOTO, 2870'
WITH start, end, start_addr, end_addr LIMIT 1

// Create a temporary GDS graph of the ENTIRE city
CALL gds.graph.project.cypher(
  'eco-routing-full-cg',
  'MATCH (n:Intersection) RETURN id(n) AS id',
  'MATCH (n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection) 
   WHERE r.eco_cost IS NOT NULL 
   RETURN id(n) AS source, id(m) AS target, r.eco_cost as weight'
) YIELD graphName

// Call Dijkstra and yield the nodeIds
CALL gds.shortestPath.dijkstra.stream(graphName, {
    sourceNode: start,
    targetNode: end,
    relationshipWeightProperty: 'weight'
})
YIELD nodeIds, totalCost

// Clean up
CALL {
  WITH graphName
  CALL gds.graph.drop(graphName) YIELD graphName AS droppedGraph
  RETURN droppedGraph
}

// **FINAL POLISH**: Format the point objects into simple [lat, lon] arrays
RETURN 
    totalCost AS total_eco_cost,
    // For each node in the path, create a simple list: [latitude, longitude]
    [nodeId IN nodeIds | [gds.util.asNode(nodeId).location.latitude, gds.util.asNode(nodeId).location.longitude]] AS route,
    start_addr.full_address AS from_address,
    end_addr.full_address AS to_address;