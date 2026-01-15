// Step 1: Drop old constraint and create new one on hash
DROP CONSTRAINT addressIdConstraint IF EXISTS;
CREATE CONSTRAINT addressHashConstraint IF NOT EXISTS FOR (a:Address) REQUIRE a.hash IS UNIQUE;
CREATE POINT INDEX addressLocationIndex IF NOT EXISTS FOR (a:Address) ON (a.location);

// Step 2: Load addresses using MERGE on hash (handles duplicates gracefully)
CALL apoc.load.json("https://myneo.blob.core.windows.net/pixote/campina_grande_addresses.geojson") YIELD value
WITH value
WHERE value.geometry IS NOT NULL 
  AND value.geometry.coordinates IS NOT NULL
  AND value.geometry.coordinates[0] IS NOT NULL
  AND value.geometry.coordinates[1] IS NOT NULL
CALL {
    WITH value
    MERGE (a:Address {hash: value.properties.hash})
    ON CREATE SET 
        a.location = point({
            longitude: toFloat(value.geometry.coordinates[0]),
            latitude: toFloat(value.geometry.coordinates[1])
        }),
        a.full_address = value.properties.street + ", " + value.properties.number + " - " +
            value.properties.city + ", " + value.properties.region,
        a.id = value.properties.id,
        a.number = value.properties.number,
        a.unit = value.properties.unit,
        a.city = value.properties.city,
        a.street = value.properties.street,
        a.district = value.properties.district,
        a.postcode = value.properties.postcode,
        a.region = value.properties.region
} IN TRANSACTIONS OF 10000 ROWS;

// Step 3: Connect each Address to its nearest Intersection
// Connect each Address to its nearest Intersection (optimized)
CALL apoc.periodic.iterate(
  'MATCH (p:Address) WHERE NOT EXISTS ((p)-[:NEAREST_INTERSECTION]->(:Intersection)) RETURN p',
  '
    WITH p
    CALL {
      WITH p
      MATCH (i:Intersection)
      WHERE point.distance(i.location, p.location) < 500
      RETURN i, point.distance(p.location, i.location) AS dist
      ORDER BY dist ASC
      LIMIT 1
    }
    WITH p, i, dist
    WHERE i IS NOT NULL
    MERGE (p)-[r:NEAREST_INTERSECTION]->(i)
    SET r.distance = dist
  ', 
  {batchSize: 1000, parallel: true, retries: 3}
) YIELD batches, total, timeTaken, committedOperations;