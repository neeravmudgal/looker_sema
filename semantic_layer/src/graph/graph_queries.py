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
