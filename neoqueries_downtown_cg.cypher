// ===================================================================
// A. CREATE THE DOWNTOWN CAMPINA GRANDE GRAPH PROJECTION
// This approach is faster and focuses analysis on the core urban area.
// Bounding box is based on the address density analysis.
// ===================================================================

// First, clean up any old graphs and properties
CALL gds.graph.drop('downtown-campina-grande', false) YIELD graphName;

MATCH (n:Intersection)
REMOVE n.downtown_betweenness, n.downtown_community;


// Now, create the projection using a Cypher query with a geographic bounding box
CALL gds.graph.project.cypher(
  'downtown-campina-grande',
  // Node query: Select intersections in the downtown area
  'MATCH (n:Intersection)
   WHERE n.location.latitude > -7.25
     AND n.location.latitude < -7.21
     AND n.location.longitude > -35.91
     AND n.location.longitude < -35.86
   RETURN id(n) AS id',
  // Relationship query: Select roads where BOTH endpoints are in the downtown area
  'MATCH (n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
   WHERE n.location.latitude > -7.25
     AND n.location.latitude < -7.21
     AND n.location.longitude > -35.91
     AND n.location.longitude < -35.86
     AND m.location.latitude > -7.25
     AND m.location.latitude < -7.21
     AND m.location.longitude > -35.91
     AND m.location.longitude < -35.86
   RETURN id(n) AS source, id(m) AS target'
) YIELD graphName AS projectedGraph, nodeCount, relationshipCount;

// ===================================================================
// B. CALCULATE BETWEENNESS CENTRALITY FOR DOWNTOWN
// ===================================================================
CALL gds.betweenness.write('downtown-campina-grande', {
    writeProperty: 'downtown_betweenness'
}) YIELD nodePropertiesWritten;


// ===================================================================
// C. CALCULATE COMMUNITY DETECTION FOR DOWNTOWN
// ===================================================================
CALL gds.louvain.write('downtown-campina-grande', {
    writeProperty: 'downtown_community'
}) YIELD nodePropertiesWritten;


// ===================================================================
// D. CLEANUP
// ===================================================================
CALL gds.graph.drop('downtown-campina-grande');

// ===================================================================
// BLOOM VISUALIZATION QUERY
// ===================================================================
/*
MATCH p=(n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
WHERE n.downtown_betweenness IS NOT NULL
  AND m.downtown_betweenness IS NOT NULL
RETURN p;
*/
