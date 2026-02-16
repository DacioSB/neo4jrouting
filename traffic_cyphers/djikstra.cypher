MATCH (start_addr:Address) 
WHERE toUpper(start_addr.full_address) CONTAINS 'JOAO SUASSUNA'
WITH start_addr LIMIT 1
MATCH (start_addr)-[:NEAREST_INTERSECTION]->(start:Intersection)

MATCH (end_addr:Address) 
WHERE toUpper(end_addr.full_address) CONTAINS 'AVENIDA FLORIANO PEIXOTO, 2870'
WITH start, end_addr LIMIT 1
MATCH (end_addr)-[:NEAREST_INTERSECTION]->(end:Intersection)

CALL gds.shortestPath.dijkstra.stream('campina-grande-traffic-analysis', {
    sourceNode: start,
    targetNode: end,
    relationshipWeightProperty: 'current_travel_time'
})
YIELD path, totalCost

WITH totalCost,
     [n IN nodes(path) | [n.location.latitude, n.location.longitude]] AS route_coords,
     [r IN relationships(path) | coalesce(r.traffic_multiplier, 1.0)] AS congestion_history
     
RETURN route_coords AS route,
       totalCost / 60.0 AS estimated_minutes,
       congestion_history,
       CASE 
         WHEN totalCost / 60.0 < 5 THEN 'Fast'
         WHEN totalCost / 60.0 < 15 THEN 'Moderate'
         ELSE 'Slow/Heavy Traffic'
       END AS traffic_condition;