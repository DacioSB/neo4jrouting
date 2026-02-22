MATCH ()-[r:ROAD_SEGMENT]->()
WHERE r.length IS NOT NULL AND r.length > 0
WITH r, 
     // Convert max_speed to a string, handling lists and nulls
     toString(
       CASE 
         WHEN r.max_speed IS NULL THEN '30' // Default if property is missing
         WHEN r.max_speed IS :: LIST<ANY> THEN r.max_speed[0]
         ELSE r.max_speed
       END
     ) AS speed_str
WITH r, r.length AS length_meters, 
     toFloat(split(speed_str, ' ')[0]) AS parsed_speed
WITH r, length_meters, parsed_speed,
     CASE 
       WHEN parsed_speed IS NULL OR isNaN(parsed_speed) OR parsed_speed <= 0 
       THEN 30.0 
       ELSE parsed_speed 
     END AS effective_speed
SET r.travel_time = (length_meters / 1000.0) / effective_speed * 3600;

MATCH ()-[r:ROAD_SEGMENT]->()
WHERE r.travel_time IS NOT NULL
SET r.traffic_multiplier = 1.0,
    r.current_travel_time = r.travel_time;

MATCH ()-[r:ROAD_SEGMENT]->()
WHERE r.highway IN ['motorway', 'trunk', 'primary']
  AND r.name IS NOT NULL
  AND r.travel_time IS NOT NULL
CALL {
  WITH r
  WITH r, rand() AS rf
  SET r.traffic_multiplier = 
    CASE 
      WHEN rf < 0.7 THEN 1.0 + (rf * 0.5)
      WHEN rf < 0.9 THEN 1.35 + (rf * 1.0)
      ELSE 2.25 + (rf * 2.0)
    END,
    r.current_travel_time = r.travel_time * r.traffic_multiplier
} IN TRANSACTIONS OF 5000 ROWS;

MATCH ()-[r:ROAD_SEGMENT]->()
WHERE r.name IS NOT NULL
WITH r, 
     CASE 
       WHEN r.name IS :: LIST<ANY> THEN toString(r.name[0])
       ELSE toString(r.name)
     END AS roadName
WHERE toUpper(roadName) CONTAINS 'FLORIANO PEIXOTO'
  AND r.travel_time IS NOT NULL AND NOT isNaN(r.travel_time)
SET r.traffic_multiplier = 3.5,
    r.current_travel_time = r.travel_time * 3.5,
    r.traffic_reason = 'Heavy congestion';

CALL gds.graph.drop('campina-grande-traffic-analysis', false) YIELD graphName;

CALL gds.graph.project(
  'campina-grande-traffic-analysis',
  ['Intersection'],
  {
    ROAD_SEGMENT: {
      orientation: 'UNDIRECTED',
      properties: ['current_travel_time', 'length', 'eco_cost']
    }
  }
) YIELD graphName, nodeCount, relationshipCount;