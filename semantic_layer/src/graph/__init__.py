"""
Neo4j Graph package.

Builds a property graph from parsed LookML and provides:
  - Schema creation (indexes, constraints, vector indexes)
  - Graph construction with batch writes
  - All Cypher queries centralized in graph_queries.py
  - In-memory ExploreContextCache for fast field lookups
"""
