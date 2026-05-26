#!/usr/bin/env python3
"""
AIOps Monitoring Agent - Synthetic Data Generator

Generates 24 hours of synthetic llm_api_calls and agent_traces events
and publishes them to Confluent Cloud Kafka.

Setup:
    cp .env.example .env
    # fill in your Kafka + Schema Registry credentials
    pip install confluent-kafka python-dotenv

Usage:
    python datagen.py
    python datagen.py --dry-run
    python datagen.py --verbose
"""

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

try:
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import SerializationContext, MessageField
except ImportError:
    print("Missing dependencies. Run: pip install confluent-kafka python-dotenv")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Avro Schemas
# ---------------------------------------------------------------------------

LLM_API_CALL_SCHEMA = json.dumps({
    "type": "record",
    "name": "llm_api_call_value",
    "namespace": "com.aiops.monitoring",
    "fields": [
        {"name": "call_id", "type": "string"},
        {"name": "team", "type": "string"},
        {"name": "feature_name", "type": "string"},
        {"name": "model", "type": "string"},
        {"name": "input_tokens", "type": "int"},
        {"name": "output_tokens", "type": "int"},
        {"name": "cost_usd", "type": "double"},
        {"name": "latency_ms", "type": "int"},
        {"name": "quality_score", "type": "double"},
        {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})

AGENT_TRACE_SCHEMA = json.dumps({
    "type": "record",
    "name": "agent_trace_value",
    "namespace": "com.aiops.monitoring",
    "fields": [
        {"name": "trace_id", "type": "string"},
        {"name": "agent_id", "type": "string"},
        {"name": "tool_name", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "error_type", "type": ["null", "string"], "default": None},
        {"name": "loop_count", "type": "int"},
        {"name": "duration_ms", "type": "int"},
        {"name": "tokens_used", "type": "int"},
        {"name": "event_ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    ],
})

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

TEAMS = ["platform", "growth", "ml-infra", "data-science", "frontend"]
FEATURES = ["recommendation", "search-ranking", "chatbot", "summarization", "classification"]
MODELS = {
    "gpt-35-turbo": {"cost_per_1k": 0.0015, "tokens_in": (50, 300),  "tokens_out": (20, 150)},
    "gpt-4":        {"cost_per_1k": 0.0300, "tokens_in": (100, 600), "tokens_out": (50, 400)},
    "gpt-4-turbo":  {"cost_per_1k": 0.0100, "tokens_in": (200, 800), "tokens_out": (100, 500)},
}
AGENT_IDS = [
    "fraud-detector-agent",
    "recommendation-agent",
    "dispute-resolver-agent",
    "onboarding-agent",
    "pricing-optimizer-agent",
]
TOOLS = ["http_get", "http_post", "vector_search", "database_query", "cache_lookup"]

WINDOW_MS = 5 * 60 * 1000
NUM_WINDOWS = 288

# Anomaly injection: window index -> affected entity
LLM_ANOMALY_WINDOWS  = {100: "growth", 180: "platform", 250: "ml-infra"}
AGENT_ANOMALY_WINDOWS = {140: "fraud-detector-agent", 220: "recommendation-agent"}


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def _make_llm_call(ts_ms: int, anomaly: bool = False, team: str = None) -> dict:
    t = (team if anomaly else None) or random.choice(TEAMS)
    m_name = "gpt-4" if anomaly else random.choices(list(MODELS.keys()), weights=[0.70, 0.20, 0.10])[0]
    m = MODELS[m_name]
    in_tok = random.randint(*m["tokens_in"])
    out_tok = random.randint(*m["tokens_out"])
    mult = random.uniform(8.0, 15.0) if anomaly else 1.0
    return {
        "call_id": str(uuid.uuid4()),
        "team": t,
        "feature_name": random.choice(FEATURES),
        "model": m_name,
        "input_tokens": in_tok,
        "output_tokens": out_tok,
        "cost_usd": round(((in_tok + out_tok) / 1000) * m["cost_per_1k"] * mult, 6),
        "latency_ms": random.randint(3000, 9000) if anomaly else random.randint(150, 2500),
        "quality_score": round(random.uniform(0.65, 0.80) if anomaly else random.uniform(0.78, 0.97), 3),
        "event_ts": ts_ms,
    }


def _make_agent_trace(ts_ms: int, anomaly: bool = False, agent_id: str = None) -> dict:
    aid = (agent_id if anomaly else None) or random.choice(AGENT_IDS)
    if anomaly:
        loop_count = random.randint(12, 45)
        status = random.choices(["error", "timeout", "loop_detected"], weights=[0.5, 0.3, 0.2])[0]
        error_type = random.choice(["ToolCallLoopError", "MaxIterationsExceeded", "TimeoutError"])
        duration_ms = loop_count * random.randint(400, 1200)
        tokens_used = loop_count * random.randint(150, 500)
    else:
        loop_count = random.randint(1, 3)
        status = random.choices(["success", "error", "timeout"], weights=[0.94, 0.04, 0.02])[0]
        error_type = None if status == "success" else random.choice(["NetworkError", "ParseError", "AuthError"])
        duration_ms = random.randint(80, 1800)
        tokens_used = random.randint(40, 350)
    return {
        "trace_id": str(uuid.uuid4()),
        "agent_id": aid,
        "tool_name": random.choice(TOOLS),
        "status": status,
        "error_type": error_type,
        "loop_count": loop_count,
        "duration_ms": duration_ms,
        "tokens_used": tokens_used,
        "event_ts": ts_ms,
    }


def generate_events() -> tuple:
    now_ms = int(time.time() * 1000)
    start_ms = (now_ms // WINDOW_MS) * WINDOW_MS - (NUM_WINDOWS * WINDOW_MS)
    llm_events, agent_events = [], []

    for w in range(NUM_WINDOWS):
        ws = start_ms + w * WINDOW_MS
        llm_anom = w in LLM_ANOMALY_WINDOWS
        agent_anom = w in AGENT_ANOMALY_WINDOWS
        for _ in range(random.randint(50, 80) if llm_anom else random.randint(10, 20)):
            llm_events.append(_make_llm_call(ws + random.randint(0, WINDOW_MS - 1), llm_anom, LLM_ANOMALY_WINDOWS.get(w)))
        for _ in range(random.randint(40, 60) if agent_anom else random.randint(15, 25)):
            agent_events.append(_make_agent_trace(ws + random.randint(0, WINDOW_MS - 1), agent_anom, AGENT_ANOMALY_WINDOWS.get(w)))

    llm_events.sort(key=lambda x: x["event_ts"])
    agent_events.sort(key=lambda x: x["event_ts"])
    return llm_events, agent_events


# ---------------------------------------------------------------------------
# Publisher
# ---------------------------------------------------------------------------

def load_credentials() -> dict:
    creds = {
        "bootstrap_servers":        os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        "kafka_api_key":            os.getenv("KAFKA_API_KEY"),
        "kafka_api_secret":         os.getenv("KAFKA_API_SECRET"),
        "schema_registry_url":      os.getenv("SCHEMA_REGISTRY_URL"),
        "schema_registry_api_key":  os.getenv("SCHEMA_REGISTRY_API_KEY"),
        "schema_registry_api_secret": os.getenv("SCHEMA_REGISTRY_API_SECRET"),
    }
    missing = [k for k, v in creds.items() if not v]
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your credentials.")
        sys.exit(1)

    # If running alongside the workshop repo, auto-populate from Terraform state
    workshop_root = Path(__file__).parent.parent.parent / "Workshops" / "quickstart-streaming-agents"
    if workshop_root.exists() and not all(creds.values()):
        try:
            sys.path.insert(0, str(workshop_root))
            from scripts.common.terraform import extract_kafka_credentials, get_project_root
            from scripts.common.cloud_detection import auto_detect_cloud_provider
            tf_creds = extract_kafka_credentials(auto_detect_cloud_provider(), workshop_root)
            creds = {
                "bootstrap_servers":          tf_creds["bootstrap_servers"],
                "kafka_api_key":              tf_creds["kafka_api_key"],
                "kafka_api_secret":           tf_creds["kafka_api_secret"],
                "schema_registry_url":        tf_creds["schema_registry_url"],
                "schema_registry_api_key":    tf_creds["schema_registry_api_key"],
                "schema_registry_api_secret": tf_creds["schema_registry_api_secret"],
            }
        except Exception:
            pass

    return creds


class AIOpsPublisher:
    def __init__(self, creds: dict):
        self.logger = logging.getLogger(__name__)
        sr = SchemaRegistryClient({
            "url": creds["schema_registry_url"],
            "basic.auth.user.info": f"{creds['schema_registry_api_key']}:{creds['schema_registry_api_secret']}",
        })
        self.llm_serializer = AvroSerializer(sr, LLM_API_CALL_SCHEMA)
        self.agent_serializer = AvroSerializer(sr, AGENT_TRACE_SCHEMA)
        self.producer = Producer({
            "bootstrap.servers": creds["bootstrap_servers"],
            "sasl.mechanisms": "PLAIN",
            "security.protocol": "SASL_SSL",
            "sasl.username": creds["kafka_api_key"],
            "sasl.password": creds["kafka_api_secret"],
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "snappy",
        })

    def publish(self, events: list, topic: str, serializer, key_field: str) -> int:
        success = 0
        for i, event in enumerate(events, 1):
            try:
                ctx = SerializationContext(topic, MessageField.VALUE)
                self.producer.produce(topic, key=event[key_field].encode(), value=serializer(event, ctx))
                success += 1
                if i % 500 == 0:
                    self.producer.poll(0)
                    self.logger.info(f"  {i}/{len(events)} -> {topic}")
            except Exception as e:
                self.logger.error(f"Publish error: {e}")
        self.producer.flush()
        return success


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="AIOps Monitoring Agent - data generator")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    logger.info("AIOps Monitoring Agent - Synthetic Data Generator")

    logger.info(f"Generating {NUM_WINDOWS} x 5-min windows (24h) of synthetic events...")
    llm_events, agent_events = generate_events()
    logger.info(f"  {len(llm_events)} LLM API calls  ({len(LLM_ANOMALY_WINDOWS)} anomaly windows: {sorted(LLM_ANOMALY_WINDOWS)})")
    logger.info(f"  {len(agent_events)} agent traces    ({len(AGENT_ANOMALY_WINDOWS)} anomaly windows: {sorted(AGENT_ANOMALY_WINDOWS)})")

    if args.dry_run:
        print("\nDRY RUN complete — no messages published.")
        return

    creds = load_credentials()
    pub = AIOpsPublisher(creds)

    logger.info("Publishing llm_api_calls...")
    llm_ok = pub.publish(llm_events, "llm_api_calls", pub.llm_serializer, "call_id")

    logger.info("Publishing agent_traces...")
    agent_ok = pub.publish(agent_events, "agent_traces", pub.agent_serializer, "trace_id")

    print(f"\n{'='*60}")
    print("AIOPS DATA PUBLISHING SUMMARY")
    print(f"{'='*60}")
    print(f"llm_api_calls:  {llm_ok}/{len(llm_events)} published")
    print(f"agent_traces:   {agent_ok}/{len(agent_events)} published")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
