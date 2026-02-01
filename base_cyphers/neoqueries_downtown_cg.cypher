// ===================================================================
// FULL CITY ANALYSIS - CAMPINA GRANDE (V6 - Salvage Method)
// This version assumes the gds.betweenness procedure is unstable
// and explicitly filters its output to salvage only valid, finite,
// non-negative scores.
// ===================================================================

// --- Step 1: Clean up any previous results and temporary labels ---
MATCH (n:Intersection)
REMOVE n.cg_betweenness, n.cg_community;

// The MATCH and REMOVE below is a separate statement.
MATCH (n:MainComponentNode)
REMOVE n:MainComponentNode;


// --- Step 2: Drop old graph projections ---
CALL gds.graph.drop('campina-grande-full', false);
CALL gds.graph.drop('campina-grande-main', false);


// --- Step 3: Project full graph to run WCC ---
CALL gds.graph.project(
  'campina-grande-full',
  'Intersection',
  { ROAD_SEGMENT: { orientation: 'UNDIRECTED' } }
) YIELD graphName;


// --- Step 4: Find the largest component and apply a temporary label to its nodes ---
CALL gds.wcc.stream('campina-grande-full')
YIELD nodeId, componentId
WITH componentId, collect(nodeId) AS nodeIds
ORDER BY size(nodeIds) DESC
LIMIT 1
WITH nodeIds
MATCH (n:Intersection) WHERE id(n) IN nodeIds
SET n:MainComponentNode
RETURN count(n) AS nodesInMainComponent;


// --- Step 5: Create a new graph projection using the temporary label ---
CALL gds.graph.project(
    'campina-grande-main',
    'MainComponentNode',
    {
        ROAD_SEGMENT: {
            orientation: 'UNDIRECTED'
        }
    }
) YIELD graphName, nodeCount, relationshipCount;


// --- Step 6: Calculate Betweenness using MUTATE and then filter the results ---
// This avoids writing erroneous values (-Infinity, NaN) directly to the DB.
CALL gds.betweenness.mutate('campina-grande-main', {
  mutateProperty: 'temp_betweenness',
  samplingSize: 2000,
  samplingSeed: 42
});

// Stream the results, filter for sane values, and write them to the actual property
CALL gds.graph.nodeProperty.stream('campina-grande-main', 'temp_betweenness')
YIELD nodeId, propertyValue
// FILTERING: Keep only finite, non-negative numbers within a reasonable range.
// This works because comparisons with NaN, Infinity, and -Infinity behave predictably.
WHERE propertyValue >= 0 AND propertyValue < 1000000000 // A reasonable upper bound of 1 billion
CALL {
    WITH nodeId, propertyValue
    MATCH (n) WHERE id(n) = nodeId
    SET n.cg_betweenness = propertyValue
} IN TRANSACTIONS OF 10000 ROWS;


// --- Step 7: Calculate Community Detection (Louvain) on the main component ---
CALL gds.louvain.write('campina-grande-main', {
  writeProperty: 'cg_community'
});


// --- Step 8: Clean up temporary data and in-memory graphs ---
// Remove the label
MATCH (n:MainComponentNode)
REMOVE n:MainComponentNode;

// Drop the graphs
CALL gds.graph.drop('campina-grande-full');
CALL gds.graph.drop('campina-grande-main');

// ===================================================================
// VERIFICATION QUERY
// ===================================================================
/*
MATCH (n:Intersection)
WHERE n.cg_betweenness IS NOT NULL
RETURN
  count(n) AS nodes_with_scores,
  min(n.cg_betweenness) AS min_score,
  max(n.cg_betweenness) AS max_score,
  avg(n.cg_betweenness) AS avg_score,
  percentileCont(n.cg_betweenness, 0.90) AS p90,
  percentileCont(n.cg_betweenness, 0.99) AS p99;
*/
