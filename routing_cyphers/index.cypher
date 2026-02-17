CREATE FULLTEXT INDEX addressSearchIndex IF NOT EXISTS
FOR (n:Address)
ON EACH [n.full_address];
