# Golden Query Tests

## What Are Golden Queries?

Golden queries are curated test fixtures that describe **expected system behavior** for representative natural-language queries. Each test encodes:

- A user question (plain English).
- The expected turn type (`answer`, `clarification`, or `no_match`).
- Optional field-name patterns the retrieval system should surface.
- Optional ambiguity metadata when clarification is required.

Tests are **generic** -- they do not hard-code view or explore names. Instead they use substring patterns (e.g. `"revenue"`) that should match fields returned by the retrieval layer regardless of the specific LookML model loaded.

## JSON Schema

Every golden query file is a single JSON object with these keys:

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `id` | string | yes | Unique identifier matching the filename prefix (e.g. `"01_simple_metric"`). |
| `description` | string | yes | Human-readable summary of what the test covers. |
| `user_query` | string | yes | The natural-language question sent to the semantic layer. |
| `expected_turn_type` | string | yes | One of `"answer"`, `"clarification"`, or `"no_match"`. |
| `expected_ambiguity_type` | string or null | no | If `expected_turn_type` is `"clarification"`, the category of ambiguity (e.g. `"attribution"`). |
| `expected_options_contain` | array of strings or null | no | Substrings that must appear in the clarification options. |
| `expected_explore` | string or null | no | If a specific explore is expected, its name. Usually `null` for generic tests. |
| `expected_fields_pattern` | array of strings or null | no | Substring patterns that should match at least one field in the generated query. `null` when no query is expected. |
| `notes` | string | no | Free-text notes for maintainers explaining edge cases or intent. |

## How to Add a New Test

1. Choose the next available numeric prefix (e.g. `16_my_test.json`).
2. Copy an existing file as a template.
3. Fill in every required field. Set optional fields to `null` when not applicable.
4. Validate the JSON (e.g. `python -m json.tool < 16_my_test.json`).
5. Add a brief note in `notes` explaining why this test exists.

Keep test queries **model-agnostic**: refer to common business concepts (revenue, sessions, users) rather than specific `view_name.field_name` paths.

## How the Test Runner Uses These Files

The test runner (`conftest.py` / `test_golden_queries.py`) performs the following steps:

1. **Discovery** -- Glob all `*.json` files in this directory sorted by filename.
2. **Deserialization** -- Parse each file into a `GoldenQuery` dataclass.
3. **Execution** -- For each test, send `user_query` through the semantic layer pipeline (retrieval, disambiguation, query generation).
4. **Assertions**:
   - `expected_turn_type` must match the actual turn type returned.
   - If `expected_fields_pattern` is set, every pattern must substring-match at least one field in the generated query.
   - If `expected_ambiguity_type` is set, the clarification response must report that ambiguity category.
   - If `expected_options_contain` is set, each substring must appear in at least one clarification option.
   - If `expected_explore` is set, the selected explore must match.
5. **Reporting** -- Passes / failures are reported per test id with details on which assertion failed.

Tests that fail only on field matching (but get the turn type right) are reported as **soft failures** to allow iterative improvement of the retrieval layer without blocking CI.
