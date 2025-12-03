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


// ===================================================================

MATCH p=(n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
WHERE n.cg_betweenness IS NOT NULL AND m.cg_betweenness IS NOT NULL
RETURN p
LIMIT 500
//Or, if you want to visualize the entire road network with all intersections that have betweenness scores:
MATCH p=(n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
WHERE n.cg_betweenness IS NOT NULL
RETURN p
//To also include community information in your perspective:
MATCH p=(n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
WHERE n.cg_betweenness IS NOT NULL AND n.cg_community IS NOT NULL
RETURN p
LIMIT 1000
//For a more focused view on high-betweenness intersections (the most important ones):
MATCH p=(n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection)
WHERE n.cg_betweenness > 1000  // Adjust threshold based on your data
RETURN p
LIMIT 1000

// ===================================================================

These queries need to be updated! You calculated **`cg_betweenness`** and **`cg_community`** for Campina Grande, but NOT **`degree_centrality`**.

**You have two options:**

---

## **Option 1: Calculate Degree Centrality first**

Add this query to your analysis (after betweenness and community detection):

```cypher
// Calculate Degree Centrality for Campina Grande
CALL gds.degree.stream('campina-grande-analysis')
YIELD nodeId, score
CALL {
  WITH nodeId, score
  MATCH (n) WHERE id(n) = nodeId
  SET n.cg_degree = score
} IN TRANSACTIONS OF 10000 ROWS;
```

Then use these updated queries:

```cypher
// Step 1: Find roads connected to highest degree intersection
MATCH (n:Intersection)
WHERE n.cg_degree IS NOT NULL
ORDER BY n.cg_degree DESC
LIMIT 1
WITH n
MATCH (n)-[r:ROAD_SEGMENT]-()
WHERE r.name IS NOT NULL
RETURN DISTINCT r.name AS roadName;

// Step 2: Visualize the hub and its neighbors
MATCH (n:Intersection)
WHERE n.cg_degree IS NOT NULL
ORDER BY n.cg_degree DESC
LIMIT 1
WITH n
MATCH (n)-[r:ROAD_SEGMENT]-(m)
RETURN n, r, m;
```

---

## **Option 2: Use Betweenness instead (no additional calculation needed)**

Since betweenness often identifies similar hubs:

```cypher
// Step 1: Find roads connected to highest betweenness intersection
MATCH (n:Intersection)
WHERE n.cg_betweenness IS NOT NULL
ORDER BY n.cg_betweenness DESC
LIMIT 1
WITH n
MATCH (n)-[r:ROAD_SEGMENT]-()
WHERE r.name IS NOT NULL
RETURN DISTINCT r.name AS roadName;

// Step 2: Visualize the hub and its neighbors
MATCH (n:Intersection)
WHERE n.cg_betweenness IS NOT NULL
ORDER BY n.cg_betweenness DESC
LIMIT 1
WITH n
MATCH (n)-[r:ROAD_SEGMENT]-(m)
RETURN n, r, m;
```

---

**Recommendation:** Use **Option 2** (betweenness) first since you already have it. If you need degree centrality specifically for your analysis, then add Option 1's calculation to your analysis script.

**Key changes:**
- `degree_centrality` → `cg_degree` (if you calculate it)
- OR use `cg_betweenness` instead (already calculated)