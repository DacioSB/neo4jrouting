This Python snippet is designed to efficiently load a large amount of data into a Neo4j graph
  database. It does this by first creating nodes (intersections) and then creating relationships
  (road segments) between them.

  Here is a detailed breakdown of each part:

  1. insert_data Function

  This function handles the core logic of sending data to the database in manageable chunks or
  "batches".

   * `def insert_data(tx, query, rows, batch_size=10000):`: Defines the function that takes four
     arguments:
       * tx: A transaction object from the database driver. All operations within a transaction are
         treated as a single atomic unit.
       * query: The Cypher query (a string) to be executed.
       * rows: The data to be inserted, which is expected to be a Pandas DataFrame.
       * batch_size: The number of rows to send to the database in a single batch. The default is
         10,000.
   * `total = 0` and `batch = 0`: Initializes two counters. total will track the total number of
     records inserted across all batches, and batch will keep track of which batch is currently
     being processed.

  The while loop

  This is the most critical part of the function, responsible for the batching process.

   * `while batch * batch_size < len(rows):`: This condition checks if there are still rows left to
     process.
       * len(rows) gives the total number of rows in the DataFrame.
       * batch * batch_size calculates how many rows have already been processed in previous loops.
       * The loop continues as long as the number of processed rows is less than the total number of
         rows.
   * `results = tx.run(...)`: This line executes the database query for the current batch.
       * `rows[batch*batch_size:(batch+1)*batch_size]`: This is a Python slice that extracts the
         current batch from the DataFrame. For example, in the first iteration (batch = 0), it gets
         rows 0 to 9,999. In the second iteration (batch = 1), it gets rows 10,000 to 19,999, and so
          on.
       * `.to_dict('records')`: This converts the sliced DataFrame batch into a list of
         dictionaries, which is the format the database driver expects.
       * `parameters={'rows': ...}`: The batch of data is passed to the query as a parameter named
         rows.
   * `total += results[0]['total']`: The Cypher queries are written to return the count of items
     processed in that batch. This line retrieves that count and adds it to the running total.
   * `batch += 1`: The batch counter is incremented, ensuring the loop processes the next chunk of
     data in the following iteration.

  2. Cypher Queries

  These are the commands written in Neo4j's query language, Cypher, to create the graph
  structure.

   * `node_query`: This query creates the Intersection nodes.
       * UNWIND $rows AS row: Takes the list of data passed in ($rows) and processes each item one
         by one, assigning it to the variable row.
       * MERGE (i:Intersection {osmid: row.osmid}): This is a powerful command that either finds an
         existing Intersection node with the given osmid or creates a new one if it doesn't exist.
         This prevents duplicate nodes.
       * SET i.location = ..., i.street_count = ...: Sets or updates properties on the node, such as
         its geographic location and the number of streets connected to it.
   * `rels_query`: This query creates the ROAD_SEGMENT relationships between the nodes.
       * MATCH (u:Intersection ...) and MATCH (v:Intersection ...): Finds the start and end nodes
         for the road segment, which were created by the node_query.
       * MERGE (u)-[r:ROAD_SEGMENT ...]->(v): Creates a directed relationship from node u to node v.
         MERGE prevents duplicate relationships between the same two nodes.
       * SET r.oneway = ..., r.name = ...: Sets properties on the relationship itself, such as the
         road name, speed limit, and length.

  3. Execution Block

  This is the main part of the script that orchestrates the entire import process.

   * `with driver.session() as session:`: Establishes a connection session with the Neo4j database.
     Using with ensures the session is automatically and safely closed afterward.
   * `session.run("CREATE CONSTRAINT ...")`: Before importing, it creates a uniqueness constraint on
     the osmid property for all Intersection nodes. This is a critical optimization that makes the
     MERGE operations much faster.
   * `session.write_transaction(insert_data, ...)`: This executes the insert_data function within a
     write transaction. It is called twice:
       1. First, to run the node_query and create all the intersection nodes from the gdf_nodes
          DataFrame.
       2. Second, to run the rels_query and create all the road segment relationships from the
          gdf_relationships DataFrame.

  This two-step process (nodes first, then relationships) is fundamental to graph database
  construction, as you cannot create a relationship without its start and end nodes already
  existing.