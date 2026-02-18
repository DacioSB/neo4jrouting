import neo4j, { Driver } from 'neo4j-driver';

const URI = "bolt://100.53.46.182:7687";
const USER = "neo4j";
const PASSWORD = "descriptions-propose-overlays";

let driver : Driver | null = null;
try {
  driver = neo4j.driver(URI, neo4j.auth.basic(USER, PASSWORD));
  console.log("Neo4j driver initialized for Vue app");
} catch (error) {
  console.error("Failed to create Neo4j driver:", error);
}

export default driver;