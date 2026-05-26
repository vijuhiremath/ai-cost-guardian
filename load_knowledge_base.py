"""
Load AIOps knowledge base documents into MongoDB Atlas with embeddings.

Usage:
    python load_knowledge_base.py

Credentials are auto-detected from the workshop credentials.env, or
set these env vars in a .env file:
    AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY
    MONGODB_URI, MONGODB_DATABASE, MONGODB_COLLECTION
"""

import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("Run: pip install python-dotenv pymongo openai")
    sys.exit(1)

try:
    import pymongo
    from pymongo import MongoClient
    from pymongo.operations import SearchIndexModel
except ImportError:
    print("Run: pip install 'pymongo[srv]'")
    sys.exit(1)

try:
    from openai import AzureOpenAI
except ImportError:
    print("Run: pip install openai")
    sys.exit(1)

# ---------------------------------------------------------------------------
# AIOps knowledge base documents
# ---------------------------------------------------------------------------

DOCUMENTS = [
    {
        "document_id": "model_routing_01",
        "chunk": (
            "Model routing optimization: GPT-4 should only be used for tasks requiring complex reasoning, "
            "multi-step logic, or nuanced language understanding. For classification, summarization, simple "
            "Q&A, and structured data extraction, GPT-3.5-Turbo delivers equivalent quality at 10-20x lower "
            "cost. A cost spike in a team's feature is often caused by routing all traffic to GPT-4 when the "
            "task complexity does not justify it. Remediation: update the model routing config to use "
            "gpt-35-turbo for that feature. Monitor quality_score after the change — if it drops below 0.75, "
            "re-evaluate the task complexity."
        ),
    },
    {
        "document_id": "model_routing_02",
        "chunk": (
            "Automatic model selection based on task complexity: implement a routing tier that analyzes "
            "input token count and task type. Inputs under 500 tokens for classification/extraction tasks "
            "route to gpt-35-turbo. Inputs over 2000 tokens or tasks tagged 'reasoning' route to gpt-4. "
            "This hybrid routing reduces average cost by 60-80% with less than 2% quality degradation on "
            "most production workloads. Cost-per-1k-tokens: gpt-4=$0.03 input/$0.06 output, "
            "gpt-35-turbo=$0.002 input/$0.002 output."
        ),
    },
    {
        "document_id": "cost_spike_01",
        "chunk": (
            "LLM cost spike root causes and remediation: the most common causes of sudden cost spikes are "
            "(1) model upgrade without cost review — a team switches from gpt-35-turbo to gpt-4 for a "
            "high-volume feature; (2) prompt inflation — prompt templates grow with added context, doubling "
            "input token counts; (3) retry storm — failed LLM calls retry with exponential backoff but "
            "without a circuit breaker, leading to 10-50x the expected call volume; (4) runaway agent loop "
            "that makes repeated LLM calls without termination. Check call_count alongside cost — if "
            "cost_usd is high but call_count is normal, it's a model/prompt issue; if call_count is also "
            "elevated, it's a retry or loop issue."
        ),
    },
    {
        "document_id": "cost_spike_02",
        "chunk": (
            "Token budget management: production LLM systems should enforce per-request token budgets. "
            "Set max_tokens on every completion request. Track input_tokens + output_tokens per call. "
            "Alert when a feature's average tokens-per-call exceeds its baseline by more than 50%. "
            "Prompt compression techniques: remove whitespace, use shorter system prompt templates, "
            "truncate retrieved context to the most relevant chunks. A 30% reduction in input tokens "
            "yields a 30% cost reduction at no quality loss for most tasks."
        ),
    },
    {
        "document_id": "agent_loop_01",
        "chunk": (
            "Agent loop detection: infinite or excessive agent loops occur when an agent's tool calls "
            "fail to make progress toward a goal. Signs: loop_count > 10 in a single trace, repeated "
            "identical tool calls, error_type = 'max_iterations_exceeded'. Root causes: (1) tool returns "
            "ambiguous results that the LLM re-queries, (2) goal is underspecified so the agent keeps "
            "refining, (3) external API returns errors that the agent retries indefinitely. "
            "Remediation: restart the agent with a fresh context and a more constrained goal prompt. "
            "Set max_iterations = 5-10 for most tasks. Log loop_count per trace for trend analysis."
        ),
    },
    {
        "document_id": "agent_loop_02",
        "chunk": (
            "Agent restart procedures: when an agent's loop_count spikes above its baseline, force a "
            "context reset. POST to the agent control API with action='restart' and include a fresh "
            "system prompt. The fresh context prevents the agent from retrying the same failed path. "
            "After restart, monitor the next 3 traces — if loop_count returns to baseline (typically 2-4), "
            "the restart was effective. If it spikes again, escalate to the team owning the agent_id and "
            "check the tool the agent was calling most frequently."
        ),
    },
    {
        "document_id": "agent_health_01",
        "chunk": (
            "Streaming agent health monitoring best practices: track four key metrics per agent in "
            "real-time: (1) loop_count — baseline 2-4 loops per trace, alert at >10; (2) error_rate — "
            "alert when more than 20% of traces in a 5-minute window have status != 'success'; "
            "(3) avg_duration_ms — alert when >3x the rolling baseline; (4) tokens_used — alert when "
            ">2x the per-trace baseline. Correlate these metrics together — a high loop_count with "
            "high error_rate suggests a broken tool call; high loop_count with low error_rate suggests "
            "the agent is working but inefficiently."
        ),
    },
    {
        "document_id": "agent_health_02",
        "chunk": (
            "Agent tool call failure patterns: the most common failure modes in production agents are "
            "(1) HTTP 429 from downstream APIs — implement exponential backoff with jitter, max 3 retries; "
            "(2) schema validation failure on tool output — the LLM receives unexpected JSON from the tool "
            "and cannot parse it, leading to a re-call loop; (3) timeout — tool calls exceeding 30s cause "
            "the agent to retry rather than fail gracefully; (4) hallucinated tool arguments — the LLM "
            "generates invalid parameter values, causing repeated call failures. Add structured output "
            "validation for all tool call arguments to prevent (4)."
        ),
    },
    {
        "document_id": "quality_score_01",
        "chunk": (
            "Quality score degradation patterns: a drop in quality_score (below 0.75) after a model "
            "downgrade indicates the task complexity exceeded the cheaper model's capability. Track "
            "quality_score as a lagging indicator — it may take 10-20 calls to establish a new baseline. "
            "If quality drops after downgrading gpt-4 to gpt-35-turbo, consider using gpt-4 for a subset "
            "of requests where the input complexity score (based on prompt length, entity count, or "
            "reasoning depth) is high. A/B testing 10% of traffic on gpt-4 while routing 90% to "
            "gpt-35-turbo gives a quality signal without the full cost."
        ),
    },
    {
        "document_id": "cost_optimization_01",
        "chunk": (
            "Feature-specific model selection guidelines: for sentiment analysis and classification, "
            "gpt-35-turbo or a fine-tuned smaller model is sufficient. For code generation and complex "
            "reasoning, gpt-4 is often required. For summarization of long documents, gpt-35-turbo-16k "
            "offers a good cost/quality tradeoff. For RAG-based Q&A, the retrieval quality matters more "
            "than the generation model — improving retrieval reduces the context needed and cuts costs. "
            "Regularly audit each feature's model assignment against its quality_score distribution to "
            "identify over-engineered assignments."
        ),
    },
    {
        "document_id": "retry_storm_01",
        "chunk": (
            "Retry storm prevention: a retry storm occurs when a large fraction of LLM requests fail "
            "simultaneously (e.g., due to a rate limit or network issue) and all clients retry at the "
            "same time. This multiplies the cost impact. Mitigation: (1) add jitter to retry intervals "
            "(random 0-2s offset), (2) implement a circuit breaker that opens after 5 consecutive errors "
            "and waits 30s before retrying, (3) set a per-feature rate limit on the LLM gateway layer, "
            "(4) use a queue-based async pattern for non-latency-sensitive workloads to absorb spikes. "
            "A retry storm is identifiable by cost_usd spiking while quality_score drops simultaneously."
        ),
    },
    {
        "document_id": "context_window_01",
        "chunk": (
            "Context window overflow in agentic systems: when an agent's accumulated context (system "
            "prompt + tool call history + conversation turns) approaches the model's context window limit, "
            "the agent begins truncating earlier context. This can cause the agent to 'forget' prior tool "
            "results and re-call the same tools, increasing loop_count and cost. Mitigation: implement "
            "context summarization — after every 5 tool calls, summarize the accumulated history into a "
            "compact representation. For gpt-35-turbo (4096 tokens), keep total context under 3000 tokens "
            "to leave room for output. Use gpt-35-turbo-16k for agents that inherently require longer "
            "context (e.g., document analysis agents)."
        ),
    },
    {
        "document_id": "observability_01",
        "chunk": (
            "Production LLM observability: instrument every LLM call with these dimensions for "
            "effective monitoring: team, feature_name, model, input_tokens, output_tokens, cost_usd, "
            "latency_ms, quality_score, and a trace_id that links multi-step agent calls. Emit these "
            "as structured events to a streaming platform (e.g., Kafka) for real-time anomaly detection. "
            "Key dashboards: cost per team per day (with week-over-week comparison), model distribution "
            "by feature, p99 latency, error rate, and quality score distribution. Set alerts at: "
            "cost >150% of 7-day moving average, error rate >5%, p99 latency >5000ms."
        ),
    },
    {
        "document_id": "rate_limit_01",
        "chunk": (
            "Rate limiting and quota management for LLM APIs: Azure OpenAI enforces TPM (tokens per "
            "minute) and RPM (requests per minute) limits per deployment. When these limits are hit, "
            "the API returns HTTP 429. To avoid quota exhaustion: (1) set per-team and per-feature "
            "soft quotas in your LLM gateway at 80% of the hard Azure limit; (2) monitor token "
            "consumption in real-time and throttle at the gateway before hitting Azure limits; "
            "(3) provision multiple deployments across regions for high-volume features; "
            "(4) use async batching for non-interactive workloads to smooth request rate. A cost spike "
            "paired with elevated latency often indicates the system is retrying after 429 errors."
        ),
    },
    {
        "document_id": "rag_optimization_01",
        "chunk": (
            "RAG retrieval quality optimization: poor retrieval is the leading cause of LLM hallucination "
            "in RAG systems. To improve retrieval: (1) chunk documents at semantic boundaries (paragraphs "
            "or sections) rather than fixed character counts; (2) use hybrid search combining vector "
            "similarity with BM25 keyword matching; (3) re-rank the top-10 retrieved chunks with a "
            "cross-encoder before sending the top-3 to the LLM; (4) add metadata filters to restrict "
            "retrieval to relevant document categories. Retrieval quality improvements reduce the LLM "
            "context needed (fewer chunks, lower tokens) and improve quality_score without changing the "
            "generation model."
        ),
    },
]

# ---------------------------------------------------------------------------
# Credential loading
# ---------------------------------------------------------------------------

load_dotenv(Path(__file__).parent / ".env")


def load_credentials() -> dict:
    creds = {
        "azure_openai_endpoint": os.getenv("AZURE_OPENAI_ENDPOINT"),
        "azure_openai_api_key":  os.getenv("AZURE_OPENAI_API_KEY"),
        "mongodb_uri":           os.getenv("MONGODB_URI"),
        "mongodb_database":      os.getenv("MONGODB_DATABASE", "aiops_knowledge"),
        "mongodb_collection":    os.getenv("MONGODB_COLLECTION", "documents"),
    }

    # Auto-detect Azure OpenAI from workshop credentials.env
    if not creds["azure_openai_endpoint"] or not creds["azure_openai_api_key"]:
        workshop_creds = Path(__file__).parent.parent.parent / "Workshops" / "quickstart-streaming-agents" / "credentials.env"
        if workshop_creds.exists():
            for line in workshop_creds.read_text().splitlines():
                if line.startswith("TF_VAR_azure_openai_endpoint_raw="):
                    creds["azure_openai_endpoint"] = line.split("=", 1)[1].strip("'\"")
                elif line.startswith("TF_VAR_azure_openai_api_key="):
                    creds["azure_openai_api_key"] = line.split("=", 1)[1].strip("'\"")
            if creds["azure_openai_endpoint"] or creds["azure_openai_api_key"]:
                print("  Auto-detected Azure OpenAI credentials from workshop credentials.env")

    # Auto-detect MongoDB URI if not set
    if not creds["mongodb_uri"]:
        creds["mongodb_uri"] = "mongodb+srv://cluster0.xhgx1kr.mongodb.net/"

    return creds


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_texts(texts: list[str], client: AzureOpenAI) -> list[list[float]]:
    print(f"  Embedding {len(texts)} documents...")
    resp = client.embeddings.create(
        model="text-embedding-ada-002",
        input=texts,
    )
    return [item.embedding for item in resp.data]


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def get_mongo_client(uri: str, username: str = None, password: str = None) -> MongoClient:
    kwargs = {"tls": True, "serverSelectionTimeoutMS": 10_000}
    if username:
        kwargs["username"] = username
    if password:
        kwargs["password"] = password
    return MongoClient(uri, **kwargs)


def ensure_vector_index(collection, index_name: str = "vector_index", dimensions: int = 1536) -> None:
    existing = list(collection.list_search_indexes())
    if any(idx.get("name") == index_name for idx in existing):
        print(f"  Vector index '{index_name}' already exists — skipping creation.")
        return

    print(f"  Creating vector search index '{index_name}' (dimensions={dimensions})...")
    index_def = SearchIndexModel(
        definition={
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": dimensions,
                    "similarity": "cosine",
                }
            ]
        },
        name=index_name,
        type="vectorSearch",
    )
    collection.create_search_index(index_def)
    print("  Index creation submitted — it will become active in ~60 seconds on Atlas.")


def load_documents(collection, docs_with_embeddings: list[dict]) -> None:
    for doc in docs_with_embeddings:
        collection.replace_one(
            {"document_id": doc["document_id"]},
            doc,
            upsert=True,
        )
    print(f"  Upserted {len(docs_with_embeddings)} documents.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=== AIOps Knowledge Base Loader ===\n")

    creds = load_credentials()

    missing = [k for k in ("azure_openai_endpoint", "azure_openai_api_key") if not creds[k]]
    if missing:
        print(f"Missing: {', '.join(missing)}")
        print("Set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_API_KEY in your .env file.")
        sys.exit(1)

    # Strip trailing slash for AzureOpenAI client
    endpoint = creds["azure_openai_endpoint"].rstrip("/")

    # Step 1: Generate embeddings
    print("Step 1: Generating embeddings via Azure OpenAI...")
    oai = AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=creds["azure_openai_api_key"],
        api_version="2024-08-01-preview",
    )
    texts = [doc["chunk"] for doc in DOCUMENTS]
    embeddings = embed_texts(texts, oai)
    docs_with_embeddings = [
        {**doc, "embedding": emb}
        for doc, emb in zip(DOCUMENTS, embeddings)
    ]
    print(f"  Generated {len(embeddings)} embeddings (dim={len(embeddings[0])}).\n")

    # Step 2: Connect to MongoDB
    print("Step 2: Connecting to MongoDB Atlas...")
    try:
        # URI may already contain credentials (e.g. mongodb+srv://user:pass@host/...)
        mongo_client = get_mongo_client(creds["mongodb_uri"])
        mongo_client.admin.command("ping")
        print("  Connected.\n")
    except Exception as e:
        print(f"  Connection failed: {e}")
        print("\nCheck MONGODB_URI in your .env file.")
        sys.exit(1)

    db_name   = creds["mongodb_database"]
    coll_name = creds["mongodb_collection"]
    db        = mongo_client[db_name]
    collection = db[coll_name]

    # Step 3: Test write access
    print(f"Step 3: Testing write access to '{db_name}.{coll_name}'...")
    try:
        collection.insert_one({"_write_test": True})
        collection.delete_one({"_write_test": True})
        print("  Write access confirmed.\n")
    except pymongo.errors.OperationFailure as e:
        print(f"  Write access denied: {e}")
        print("\n  The connected user is read-only on this cluster.")
        print("  Update pipeline.sql Step 3.5 to use the existing Lab 2 collection:")
        print("    'mongodb.database' = 'vector_search'")
        print("    'mongodb.collection' = 'documents'")
        print("    'mongodb.index' = 'vector_index'")
        sys.exit(1)

    # Step 4: Load documents
    print(f"Step 4: Loading {len(DOCUMENTS)} AIOps knowledge base documents...")
    load_documents(collection, docs_with_embeddings)
    print()

    # Step 5: Create vector search index
    print("Step 5: Ensuring vector search index exists...")
    ensure_vector_index(collection, index_name="vector_index", dimensions=1536)
    print()

    print("=== Done! ===\n")
    print("Flink SQL to create the knowledge base table (run before Step 4 in pipeline.sql):")
    print()
    print(f"""CREATE TABLE documents_vectordb_aiops (
    document_id STRING,
    chunk       STRING,
    embedding   ARRAY<FLOAT>
) WITH (
    'connector'                = 'mongodb',
    'mongodb.connection'       = 'mongodb-connection',
    'mongodb.database'         = '{db_name}',
    'mongodb.collection'       = '{coll_name}',
    'mongodb.index'            = 'vector_index',
    'mongodb.embedding_column' = 'embedding',
    'mongodb.numCandidates'    = '500'
);""")


if __name__ == "__main__":
    main()
