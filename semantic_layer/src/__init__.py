"""
GraphRAG Semantic Layer — top-level source package.

This package contains all backend services that power the semantic layer:
  parser/      → Reads and normalizes any LookML files into Python objects
  graph/       → Builds and queries a Neo4j property graph from parsed LookML
  embeddings/  → Generates vector embeddings for fields and explores
  retrieval/   → Orchestrates ANN search + graph traversal to find relevant fields
  llm/         → Unified interface to OpenAI, Anthropic, and Google LLMs
  query_generator/ → Assembles validated Looker Explore query JSON
  conversation/    → Manages multi-turn chat state and routing
"""
