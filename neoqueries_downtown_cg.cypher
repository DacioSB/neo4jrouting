CALL gds.graph.project.cypher(
  'downtown-campina-grande',
  'MATCH (n:Intersection) 
   WHERE n.location.latitude > -7.25 AND n.location.latitude < -7.21 
     AND n.location.longitude > -35.91 AND n.location.longitude < -35.86 
   RETURN id(n) AS id',
  'MATCH (n:Intersection)-[r:ROAD_SEGMENT]-(m:Intersection) 
   WHERE n.location.latitude > -7.25 AND n.location.latitude < -7.21 
     AND n.location.longitude > -35.91 AND n.location.longitude < -35.86 
     AND m.location.latitude > -7.25 AND m.location.latitude < -7.21 
     AND m.location.longitude > -35.91 AND m.location.longitude < -35.86 
   RETURN id(n) AS source, id(m) AS target'
) YIELD graphName, nodeCount, relationshipCount;

CALL gds.betweenness.stream('downtown-campina-grande')
YIELD nodeId, score
CALL {
  WITH nodeId, score
  MATCH (n) WHERE id(n) = nodeId
  SET n.downtown_betweenness = score
} IN TRANSACTIONS OF 5000 ROWS;

CALL gds.louvain.stream('downtown-campina-grande')
YIELD nodeId, communityId
CALL {
  WITH nodeId, communityId
  MATCH (n) WHERE id(n) = nodeId
  SET n.downtown_community = communityId
} IN TRANSACTIONS OF 5000 ROWS;

CALL gds.graph.drop('downtown-campina-grande');