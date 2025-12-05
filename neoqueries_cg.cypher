// ===================================================================
// STEP 1: CLEANUP ANY CORRUPTED DATA
// ===================================================================
MATCH (n:Intersection)
REMOVE n.cg_betweenness, n.cg_community;

CALL gds.graph.drop('campina-grande-analysis', false);

// ===================================================================
// STEP 2: CREATE GRAPH PROJECTION
// ===================================================================
CALL gds.graph.project(
  'campina-grande-analysis',
  'Intersection',
  {
    ROAD_SEGMENT: {
      orientation: 'UNDIRECTED'
    }
  }
) YIELD graphName, nodeCount, relationshipCount;

// ===================================================================
// STEP 3: CALCULATE BETWEENNESS WITH SAMPLING AND FILTERING
// ===================================================================
// Use MUTATE to calculate without writing yet
CALL gds.betweenness.mutate('campina-grande-analysis', {
  mutateProperty: 'betweenness_temp',
  samplingSize: 2000,
  samplingSeed: 42
}) YIELD nodePropertiesWritten, computeMillis;

// Stream results and filter out invalid values before writing
CALL gds.graph.nodeProperty.stream('campina-grande-analysis', 'betweenness_temp')
YIELD nodeId, propertyValue
WHERE propertyValue IS NOT NULL
  AND propertyValue >= 0           // Only positive values
  AND propertyValue < 1e9          // Reasonable upper limit
CALL {
  WITH nodeId, propertyValue
  MATCH (n) WHERE id(n) = nodeId
  SET n.cg_betweenness = propertyValue
} IN TRANSACTIONS OF 5000 ROWS;

// ===================================================================
// STEP 4: CALCULATE COMMUNITY DETECTION
// ===================================================================
CALL gds.louvain.write('campina-grande-analysis', {
  writeProperty: 'cg_community'
}) YIELD nodePropertiesWritten, communityCount;

// ===================================================================
// STEP 5: CLEANUP
// ===================================================================
CALL gds.graph.drop('campina-grande-analysis');