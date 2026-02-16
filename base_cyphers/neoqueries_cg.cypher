// ===================================================================
// A. CREATE THE FULL GRAPH PROJECTION
// ===================================================================
CALL gds.graph.drop('campina-grande-analysis') YIELD graphName;

CALL gds.graph.project(
  'campina-grande-analysis',
  'Intersection',
  {
    ROAD_SEGMENT: {
      orientation: 'UNDIRECTED'
    }
  }
) YIELD graphName AS projectedGraph, nodeCount, relationshipCount;


// ===================================================================
// B. CALCULATE BETWEENNESS CENTRALITY FOR THE WHOLE CITY
// ===================================================================
CALL gds.betweenness.stream('campina-grande-analysis')
YIELD nodeId, score
CALL {
  WITH nodeId, score
  MATCH (n) WHERE id(n) = nodeId
  SET n.cg_betweenness = score
} IN TRANSACTIONS OF 10000 ROWS;


// ===================================================================
// C. CALCULATE COMMUNITY DETECTION FOR THE WHOLE CITY
// ===================================================================
CALL gds.louvain.stream('campina-grande-analysis')
YIELD nodeId, communityId
CALL {
  WITH nodeId, communityId
  MATCH (n) WHERE id(n) = nodeId
  SET n.cg_community = communityId
} IN TRANSACTIONS OF 10000 ROWS;


// ===================================================================
// D. CLEANUP
// ===================================================================
CALL gds.graph.drop('campina-grande-analysis');