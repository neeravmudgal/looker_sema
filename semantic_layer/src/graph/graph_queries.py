"""
All Cypher queries used by the system, in one centralized file.

WHY: Scattering Cypher across Python files makes queries hard to find,
     debug, and optimize. Centralizing them here means:
     - One place to look when a query is slow
     - One place to audit for injection risks
     - Consistent naming convention

CALLED BY: graph_builder.py (write queries), retriever.py & cache.py (read queries).
CALLS: Nothing — this is a constants module.

NAMING CONVENTION: VERB_NOUN (e.g. FIND_FIELDS, GET_EXPLORE, CREATE_MODEL)
"""

# ═══════════════════════════════════════════════════════════════════════
# WRITE QUERIES — used by graph_builder.py to populate the graph
# ═══════════════════════════════════════════════════════════════════════

CREATE_MODELS = """
UNWIND $models AS m
MERGE (model:Model {name: m.name})
SET model.connection = m.connection,
    model.file_path  = m.file_path
"""

CREATE_VIEWS = """
UNWIND $views AS v
MERGE (view:View {name: v.name})
SET view.sql_table_name    = v.sql_table_name,
    view.derived_table_sql = v.derived_table_sql,
    view.is_pdt            = v.is_pdt,
    view.view_label        = v.view_label
"""

CREATE_EXPLORES = """
UNWIND $explores AS e
MERGE (explore:Explore {name: e.name, model_name: e.model_name})
SET explore.label              = e.label,
    explore.description        = e.description,
    explore.base_view          = e.base_view,
    explore.is_hidden          = e.is_hidden,
    explore.always_filter_json = e.always_filter_json,
    explore.tags               = e.tags
"""

CREATE_FIELDS = """
UNWIND $fields AS f
MERGE (field:Field {name: f.name, view_name: f.view_name, explore_name: f.explore_name})
SET field.field_type   = f.field_type,
    field.data_type    = f.data_type,
    field.sql          = f.sql,
    field.label        = f.label,
    field.description  = f.description,
    field.tags         = f.tags,
    field.is_hidden    = f.is_hidden,
    field.value_format = f.value_format,
    field.timeframes   = f.timeframes,
    field.model_name   = f.model_name
"""

# ── Relationship creation ─────────────────────────────────────────────

LINK_MODEL_TO_EXPLORE = """
UNWIND $links AS l
MATCH (m:Model {name: l.model_name})
MATCH (e:Explore {name: l.explore_name, model_name: l.model_name})
MERGE (m)-[:HAS_EXPLORE]->(e)
"""

LINK_EXPLORE_TO_BASE_VIEW = """
UNWIND $links AS l
MATCH (e:Explore {name: l.explore_name, model_name: l.model_name})
MATCH (v:View {name: l.view_name})
MERGE (e)-[:BASE_VIEW]->(v)
"""

LINK_EXPLORE_JOINS = """
UNWIND $joins AS j
MATCH (e:Explore {name: j.explore_name, model_name: j.model_name})
MATCH (v:View {name: j.view_name})
MERGE (e)-[r:JOINS]->(v)
SET r.sql_on       = j.sql_on,
    r.join_type    = j.join_type,
    r.relationship = j.relationship
"""

LINK_VIEW_TO_FIELD = """
UNWIND $links AS l
MATCH (v:View {name: l.view_name})
MATCH (f:Field {name: l.field_name, view_name: l.view_name, explore_name: l.explore_name})
MERGE (v)-[:HAS_FIELD]->(f)
"""

# CAN_ACCESS is the critical edge — it connects each explore directly to
# every field accessible from it. This makes the key retrieval query fast:
# "give me all fields reachable from this explore" = one hop.
LINK_EXPLORE_CAN_ACCESS = """
UNWIND $links AS l
MATCH (e:Explore {name: l.explore_name, model_name: l.model_name})
MATCH (f:Field {name: l.field_name, view_name: l.view_name, explore_name: l.explore_name})
MERGE (e)-[:CAN_ACCESS]->(f)
"""

LINK_VIEW_EXTENDS = """
UNWIND $links AS l
MATCH (child:View {name: l.child_name})
MATCH (parent:View {name: l.parent_name})
MERGE (child)-[:EXTENDS]->(parent)
"""


# ═══════════════════════════════════════════════════════════════════════
# READ QUERIES — used by retriever.py, cache.py, and UI components
# ═══════════════════════════════════════════════════════════════════════

# ── Vector search (ANN) ───────────────────────────────────────────────

ANN_SEARCH_FIELDS = """
CALL db.index.vector.queryNodes('field_embeddings', $k, $embedding)
YIELD node AS field, score
WHERE field.is_hidden = false
RETURN field.name        AS field_name,
       field.view_name   AS view_name,
       field.explore_name AS explore_name,
       field.field_type  AS field_type,
       field.data_type   AS data_type,
       field.label       AS label,
       field.description AS description,
       field.tags        AS tags,
       field.sql         AS sql,
       field.model_name  AS model_name,
       score
ORDER BY score DESC
"""

# Pre-filtered variant: only search measures or dimensions
ANN_SEARCH_FIELDS_FILTERED = """
CALL db.index.vector.queryNodes('field_embeddings', $k, $embedding)
YIELD node AS field, score
WHERE field.is_hidden = false
  AND ($field_type IS NULL OR field.field_type = $field_type)
RETURN field.name        AS field_name,
       field.view_name   AS view_name,
       field.explore_name AS explore_name,
       field.field_type  AS field_type,
       field.data_type   AS data_type,
       field.label       AS label,
       field.description AS description,
       field.tags        AS tags,
       field.sql         AS sql,
       field.model_name  AS model_name,
       score
ORDER BY score DESC
"""

ANN_SEARCH_EXPLORES = """
CALL db.index.vector.queryNodes('explore_embeddings', $k, $embedding)
YIELD node AS explore, score
WHERE explore.is_hidden = false
RETURN explore.name        AS explore_name,
       explore.model_name  AS model_name,
       explore.label       AS label,
       explore.description AS description,
       score
ORDER BY score DESC
"""

# ── View vector search ───────────────────────────────────────────────
ANN_SEARCH_VIEWS = """
CALL db.index.vector.queryNodes('view_embeddings', $k, $embedding)
YIELD node AS view, score
RETURN view.name AS view_name,
       view.view_label AS view_label,
       score
ORDER BY score DESC
"""

# ── Full-text search (hybrid complement to vector search) ────────────
# Exact keyword matching: finds fields by name/label/description keywords.
# Vector search finds "semantically similar" — fulltext finds "literally contains".
# Together they cover both fuzzy intent and precise field name references.

FULLTEXT_SEARCH_FIELDS = """
CALL db.index.fulltext.queryNodes('field_fulltext', $query)
YIELD node AS field, score
WHERE field.is_hidden = false
RETURN field.name        AS field_name,
       field.view_name   AS view_name,
       field.explore_name AS explore_name,
       field.field_type  AS field_type,
       field.data_type   AS data_type,
       field.label       AS label,
       field.description AS description,
       field.tags        AS tags,
       field.sql         AS sql,
       field.model_name  AS model_name,
       score
ORDER BY score DESC
LIMIT $k
"""

# ── GraphRAG: Graph traversal from vector-matched fields ─────────────
# VectorCypherRetriever pattern: after ANN finds candidate fields,
# traverse the graph to pull in sibling fields from the same view.
# This enriches retrieval with structured context that pure vector
# search misses — e.g. finding "revenue" also surfaces "net_profit"
# and "ROI" from the same view.

TRAVERSE_FROM_FIELD = """
MATCH (matched:Field {name: $field_name, view_name: $view_name, explore_name: $explore_name})
MATCH (v:View {name: matched.view_name})-[:HAS_FIELD]->(sibling:Field)
WHERE sibling.is_hidden = false
  AND sibling.explore_name = $explore_name
  AND sibling.name <> matched.name
RETURN sibling.name        AS field_name,
       sibling.view_name   AS view_name,
       sibling.explore_name AS explore_name,
       sibling.field_type  AS field_type,
       sibling.data_type   AS data_type,
       sibling.label       AS label,
       sibling.description AS description,
       sibling.tags        AS tags,
       sibling.sql         AS sql,
       sibling.model_name  AS model_name
ORDER BY sibling.field_type, sibling.name
"""

# ── Explore context (for cache building and context assembly) ─────────

GET_ALL_EXPLORES = """
MATCH (e:Explore)
RETURN e.name        AS name,
       e.model_name  AS model_name,
       e.label       AS label,
       e.description AS description,
       e.base_view   AS base_view,
       e.is_hidden   AS is_hidden,
       e.always_filter_json AS always_filter_json,
       e.tags        AS tags
"""

GET_EXPLORE_JOINS = """
MATCH (e:Explore {name: $explore_name, model_name: $model_name})-[j:JOINS]->(v:View)
RETURN v.name       AS view_name,
       j.sql_on     AS sql_on,
       j.join_type  AS join_type,
       j.relationship AS relationship,
       v.is_pdt     AS is_pdt
"""

GET_FIELDS_IN_EXPLORE = """
MATCH (e:Explore {name: $explore_name, model_name: $model_name})-[:CAN_ACCESS]->(f:Field)
WHERE f.is_hidden = false
RETURN f.name        AS name,
       f.view_name   AS view_name,
       f.field_type  AS field_type,
       f.data_type   AS data_type,
       f.sql         AS sql,
       f.label       AS label,
       f.description AS description,
       f.tags        AS tags,
       f.is_hidden   AS is_hidden,
       f.value_format AS value_format,
       f.timeframes  AS timeframes,
       f.model_name  AS model_name
ORDER BY f.view_name, f.name
"""

GET_ALL_FIELDS_FOR_CACHE = """
MATCH (e:Explore)-[:CAN_ACCESS]->(f:Field)
RETURN f.name         AS name,
       f.view_name    AS view_name,
       f.explore_name AS explore_name,
       f.field_type   AS field_type,
       f.data_type    AS data_type,
       f.sql          AS sql,
       f.label        AS label,
       f.description  AS description,
       f.tags         AS tags,
       f.is_hidden    AS is_hidden,
       f.value_format AS value_format,
       f.model_name   AS model_name,
       e.name         AS explore_name_from_rel
"""

# ── Diagnostics ───────────────────────────────────────────────────────

COUNT_NODES = """
MATCH (n)
RETURN labels(n)[0] AS label, count(n) AS count
ORDER BY label
"""

COUNT_RELATIONSHIPS = """
MATCH ()-[r]->()
RETURN type(r) AS rel_type, count(r) AS count
ORDER BY rel_type
"""

GET_FIELDS_WITH_EMBEDDINGS = """
MATCH (f:Field)
WHERE f.embedding IS NOT NULL
RETURN count(f) AS count
"""

GET_EXPLORES_WITH_EMBEDDINGS = """
MATCH (e:Explore)
WHERE e.embedding IS NOT NULL
RETURN count(e) AS count
"""
