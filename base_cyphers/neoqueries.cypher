// ===================================================================
// A. CREATE THE DOWNTOWN GRAPH PROJECTION
// We will create an in-memory graph containing ONLY the intersections
// in the downtown San Mateo bounding box.
// ===================================================================

// First, clean up any old graphs from our previous attempts
CALL gds.graph.drop('downtown-san-mateo') YIELD graphName;

// Now, create the projection using a Cypher query
CALL gds.graph.project.cypher(
  'downtown-san-mateo',
  // Node query: Select all intersections within the geographic box
  'MATCH (n:Intersection) WHERE n.location.latitude > 37.56 AND n.location.latitude < 37.58 AND n.location.longitude > -122.33 AND n.location.longitude < -122.31 RETURN id(n) AS id',
  // Relationship query: Select relationships where BOTH nodes are in the box
  'MATCH (n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection) WHERE n.location.latitude > 37.56 AND n.location.latitude < 37.58 AND n.location.longitude > -122.33 AND n.location.longitude < -122.31 AND m.location.latitude > 37.56 AND m.location.latitude < 37.58 AND m.location.longitude > -122.33 AND m.location.longitude < -122.31 RETURN id(n) AS source, id(m) AS target'
) YIELD graphName AS projectedGraph, nodeCount, relationshipCount;


// ===================================================================
// B. CALCULATE BETWEENNESS CENTRALITY FOR DOWNTOWN
// This identifies the key bottlenecks in the downtown area.
// ===================================================================
CALL gds.betweenness.stream('downtown-san-mateo')
YIELD nodeId, score
// Stream the results and write them back to the main graph
CALL {
  WITH nodeId, score
  MATCH (n) WHERE id(n) = nodeId
  SET n.downtown_betweenness = score
} IN TRANSACTIONS OF 5000 ROWS;


// ===================================================================
// C. CALCULATE COMMUNITY DETECTION FOR DOWNTOWN
// This finds the natural "neighborhoods" or clusters in the road network.
// ===================================================================
CALL gds.louvain.stream('downtown-san-mateo')
YIELD nodeId, communityId
// Stream the results and write them back to the main graph
CALL {
  WITH nodeId, communityId
  MATCH (n) WHERE id(n) = nodeId
  SET n.downtown_community = communityId
} IN TRANSACTIONS OF 5000 ROWS;


// ===================================================================
// D. CLEANUP
// ===================================================================
CALL gds.graph.drop('downtown-san-mateo');