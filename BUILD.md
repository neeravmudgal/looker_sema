# Build & Run Instructions

Local development setup for the GraphRAG Semantic Layer. This guide gets you from zero to a running system using **free, local models** (no API keys required).

---

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| **Python** | 3.10+ | Runtime |
| **Docker** | 20.10+ | Runs Neo4j graph database |
| **Ollama** | Latest | Runs local LLM and embedding models |

---

## Step 1: Install Ollama and Pull Models

Ollama runs LLM and embedding models locally. No API keys, no cost, fully private.

**Install Ollama:**
```bash
# macOS
brew install ollama

# Or download from https://ollama.com
```

**Start the Ollama server** (runs in background):
```bash
ollama serve
```

**Pull the required models:**
```bash
# LLM for intent extraction and query generation (~4.9 GB)
ollama pull llama3.1:8b

# Embedding model for semantic search (~274 MB)
ollama pull nomic-embed-text
```

**Verify models are available:**
```bash
ollama list
```

You should see both `llama3.1:8b` and `nomic-embed-text` in the list.

**Recommended models by hardware:**

| RAM | LLM Model | Pull Command |
|-----|-----------|-------------|
| 8 GB | `llama3.2` (3B) | `ollama pull llama3.2` |
| 16 GB | `llama3.1:8b` (8B) | `ollama pull llama3.1:8b` |
| 32 GB+ | `qwen2.5:32b` (32B) | `ollama pull qwen2.5:32b` |

Larger models produce better intent extraction and query generation. The 8B model is the minimum for reliable JSON output.

---

## Step 2: Start Neo4j via Docker

Neo4j stores the knowledge graph (LookML fields, explores, joins) and provides native vector search for embeddings.

**Pull the Neo4j image:**
```bash
docker pull neo4j:5.26.0-community
```

**Start Neo4j:**
```bash
docker run -d \
  --name semantic-layer-neo4j \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_AUTH=neo4j/semantic_layer_dev \
  -e NEO4J_PLUGINS='["apoc"]' \
  -e NEO4J_dbms_security_procedures_unrestricted='apoc.*' \
  -v neo4j_sema_data:/data \
  -v neo4j_sema_logs:/logs \
  neo4j:5.26.0-community
```

This starts:
- **Neo4j Browser UI** at [http://localhost:7474](http://localhost:7474)
- **Bolt protocol** at `bolt://localhost:7687` (what the Python app connects to)

**Verify Neo4j is running:**
```bash
docker ps --filter name=semantic-layer-neo4j
```

You should see the container with status `Up`.

**Default credentials:**
- Username: `neo4j`
- Password: `semantic_layer_dev`

**Useful commands:**
```bash
# Stop Neo4j (data preserved in volumes)
docker stop semantic-layer-neo4j

# Start it again
docker start semantic-layer-neo4j

# Stop and remove container (data preserved in volumes)
docker rm -f semantic-layer-neo4j

# Full reset (delete container + data volumes)
docker rm -f semantic-layer-neo4j
docker volume rm neo4j_sema_data neo4j_sema_logs
```

---

## Step 3: Python Environment

```bash
cd semantic_layer

# Create virtual environment
python -m venv .venv
source .venv/bin/activate    # macOS/Linux
# .venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## Step 4: Configure Environment

The `.env` file in `semantic_layer/` controls all configuration. For local-only development, the defaults work out of the box.

**Key settings to verify:**

```bash
# .env file — these should match your setup

# Neo4j (must match docker-compose.yml)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=viranchi          # Change if you modified docker-compose.yml

# Ollama (local models)
EMBEDDING_PROVIDER=ollama
EMBEDDING_MODEL=nomic-embed-text
EMBEDDING_DIMENSIONS=768          # nomic-embed-text uses 768 dimensions

DEFAULT_LLM_PROVIDER=ollama
DEFAULT_LLM_MODEL=llama3.1:8b    # Must match a model from `ollama list`

# LookML source
LOOKML_DIR=../looker_fixtures     # Relative to semantic_layer/
```

**Important:** The Neo4j password in `.env` must match what you set in the `docker run` command (`NEO4J_AUTH=neo4j/<password>`). The default is `semantic_layer_dev`.

---

## Step 5: Run the Application

```bash
cd semantic_layer
streamlit run app/streamlit_app.py
```

The app opens at [http://localhost:8501](http://localhost:8501).

**First run takes 1-2 minutes** because it:

1. Connects to Neo4j
2. Creates schema (constraints, indexes, vector indexes)
3. Parses all `.lkml` files from `looker_fixtures/`
4. Builds the knowledge graph in Neo4j
5. Generates embeddings for all fields and explores (Ollama API calls)
6. Builds in-memory cache
7. Initializes LLM and services

A progress bar shows each stage. Subsequent runs reuse the session state and are instant.

---

## Step 6: Verify Everything Works

After the app loads:

1. **Sidebar shows green status** — "Ready" with explore/field counts
2. **Type a test query:** `What is the total revenue?`
3. **Expected result:** A Looker query JSON with `session_purchase_facts.revenue` or similar
4. **Check confidence:** Should be 70%+ for simple queries

**If the sidebar shows an error:**
- Check Neo4j is running: `docker compose ps`
- Check Ollama is running: `ollama list`
- Check `.env` credentials match `docker-compose.yml`

---

## Troubleshooting

### Neo4j won't connect
```
ServiceUnavailable: Unable to retrieve routing information
```
- Ensure Docker container is running: `docker ps --filter name=semantic-layer-neo4j`
- Check port 7687 isn't used by another process: `lsof -i :7687`
- Verify credentials in `.env` match what you passed to `docker run` (`NEO4J_AUTH=neo4j/semantic_layer_dev`)

### Ollama embedding fails
```
EmbeddingError: Embedding failed after 3 attempts
```
- Ensure Ollama is running: `ollama serve` (in a separate terminal)
- Verify the embedding model is pulled: `ollama list` should show `nomic-embed-text`
- Check `EMBEDDING_DIMENSIONS=768` in `.env` (nomic-embed-text uses 768, not 1536)

### Vector index dimension mismatch
```
Cannot create vector index: dimension mismatch
```
- This happens if you switch embedding models without clearing Neo4j
- Fix: Remove container and volumes, then re-run `docker run`:
  ```bash
  docker rm -f semantic-layer-neo4j
  docker volume rm neo4j_sema_data neo4j_sema_logs
  ```
  Then run the `docker run` command from Step 2 again.

### LLM returns invalid JSON
```
LLMParseError: LLM returned invalid JSON
```
- Small models (3B) struggle with JSON. Use `llama3.1:8b` minimum
- If using `qwen3:8b`, it may emit `<think>` tags. Switch to `llama3.1:8b`

### Streamlit port conflict
```
Address already in use
```
- Use a different port: `streamlit run app/streamlit_app.py --server.port 8502`

---

## Cloud Provider Setup (Optional)

For better accuracy, you can use cloud LLMs instead of local Ollama models.

### OpenAI

```bash
# In .env
OPENAI_API_KEY=sk-...
DEFAULT_LLM_PROVIDER=openai
DEFAULT_LLM_MODEL=gpt-4o-mini       # Cheapest, great for structured output

# For embeddings too (higher quality than local)
EMBEDDING_PROVIDER=openai
EMBEDDING_MODEL=text-embedding-3-small
EMBEDDING_DIMENSIONS=1536
```

### Anthropic

```bash
# In .env
ANTHROPIC_API_KEY=sk-ant-...
DEFAULT_LLM_PROVIDER=anthropic
DEFAULT_LLM_MODEL=claude-sonnet-4-5-20250514
```

Note: Anthropic does not provide an embedding API. Use OpenAI or Ollama for embeddings.

### Google Gemini

```bash
# In .env
GOOGLE_API_KEY=AI...
DEFAULT_LLM_PROVIDER=google
DEFAULT_LLM_MODEL=gemini-2.0-flash

# For embeddings
EMBEDDING_PROVIDER=google
EMBEDDING_MODEL=text-embedding-004
EMBEDDING_DIMENSIONS=768
```

**After changing providers:** Restart the Streamlit app. If you changed the embedding provider or dimensions, also reset Neo4j (remove container + volumes and re-run `docker run` from Step 2).

---

## Running Tests

```bash
cd semantic_layer

# Unit tests (no external dependencies)
pytest tests/unit/ -v

# Integration tests (requires running Neo4j + Ollama)
pytest tests/integration/ -v

# All tests
pytest -v

# Specific test file
pytest tests/unit/test_parser.py -v

# With output
pytest -v -s
```

Integration tests use the golden query fixtures in `tests/golden_queries/` and require both Neo4j and an LLM provider to be running.

---

## Quick Reference

| Action | Command |
|--------|---------|
| Start Neo4j | `docker start semantic-layer-neo4j` |
| Stop Neo4j | `docker stop semantic-layer-neo4j` |
| Reset Neo4j data | `docker rm -f semantic-layer-neo4j && docker volume rm neo4j_sema_data neo4j_sema_logs` then re-run the `docker run` command from Step 2 |
| Start Ollama | `ollama serve` |
| Run app | `cd semantic_layer && streamlit run app/streamlit_app.py` |
| Run tests | `cd semantic_layer && pytest -v` |
| Check Neo4j UI | [http://localhost:7474](http://localhost:7474) |
| Check app | [http://localhost:8501](http://localhost:8501) |
