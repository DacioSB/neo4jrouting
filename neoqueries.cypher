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
CALL gds.betweenness.write('downtown-san-mateo', {
  writeProperty: 'downtown_betweenness'
}) YIELD nodePropertiesWritten, centralityDistribution;


// ===================================================================
// C. CALCULATE COMMUNITY DETECTION FOR DOWNTOWN
// This finds the natural "neighborhoods" or clusters in the road network.
// ===================================================================
CALL gds.louvain.write('downtown-san-mateo', {
  writeProperty: 'downtown_community'
}) YIELD nodePropertiesWritten, communityCount, communityDistribution;


// ===================================================================
// D. CLEANUP
// ===================================================================
CALL gds.graph.drop('downtown-san-mateo');


// Step 1: Find the single intersection with the highest degree centrality score
MATCH (n:Intersection)
ORDER BY n.degree_centrality DESC
LIMIT 1

// Step 2: With that specific node, find all connected road segments
WITH n
MATCH (n)-[r:ROAD_SEGMENT]-()

// Step 3: Return the unique names of those roads, filtering out any nulls
WHERE r.name IS NOT NULL
RETURN DISTINCT r.name AS roadName