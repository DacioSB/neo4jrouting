MATCH (u:Intersection)-[r:ROAD_SEGMENT]->(v:Intersection)
WHERE u.elevation IS NOT NULL AND v.elevation IS NOT NULL AND r.length > 0
WITH r, (v.elevation - u.elevation) AS delta_elevation, ( (v.elevation - u.elevation) / r.length ) AS gradient
WITH r, gradient,
     CASE WHEN r.highway IN ['motorway', 'trunk', 'primary'] THEN 1.0 ELSE 1.5 END AS road_type_factor,
     CASE WHEN gradient > 0.02 THEN (1 + (gradient * 10)) ELSE 1.0 END AS gradient_penalty
SET r.eco_cost = r.length * road_type_factor * gradient_penalty;