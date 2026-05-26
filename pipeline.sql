-- =============================================================================
-- AIOps Monitoring Agent - Flink SQL Pipeline
-- Run each statement in order in the Confluent Cloud Flink SQL workspace
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1: Create input tables from Kafka topics
-- -----------------------------------------------------------------------------

CREATE TABLE llm_api_calls (
    call_id STRING,
    team STRING,
    feature_name STRING,
    model STRING,
    input_tokens INT,
    output_tokens INT,
    cost_usd DOUBLE,
    latency_ms INT,
    quality_score DOUBLE,
    event_ts TIMESTAMP_LTZ(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'kafka.topic' = 'llm_api_calls',
    'format' = 'avro-confluent',
    'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE agent_traces (
    trace_id STRING,
    agent_id STRING,
    tool_name STRING,
    status STRING,
    error_type STRING,
    loop_count INT,
    duration_ms INT,
    tokens_used INT,
    event_ts TIMESTAMP_LTZ(3),
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'kafka.topic' = 'agent_traces',
    'format' = 'avro-confluent',
    'scan.startup.mode' = 'earliest-offset'
);


-- -----------------------------------------------------------------------------
-- STEP 2A: Detect LLM cost anomalies (5-minute tumbling windows per team+feature)
-- -----------------------------------------------------------------------------

CREATE TABLE llm_cost_anomalies (
    PRIMARY KEY (team, feature_name) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    team,
    feature_name,
    window_time,
    SUM(cost_usd)     AS total_cost_usd,
    COUNT(*)          AS call_count,
    AVG(cost_usd)     AS avg_cost_usd,
    is_anomaly,
    lower_bound,
    upper_bound
FROM TABLE(
    ML_DETECT_ANOMALIES(
        TABLE llm_api_calls,
        DESCRIPTOR(event_ts),
        DESCRIPTOR(cost_usd),
        INTERVAL '5' MINUTE,
        'team, feature_name',
        'sensitivity' = '0.95',
        'minTrainingSize' = '10'
    )
)
WHERE is_anomaly = TRUE;


-- -----------------------------------------------------------------------------
-- STEP 2B: Detect agent health anomalies (5-minute windows per agent_id)
-- -----------------------------------------------------------------------------

CREATE TABLE agent_health_anomalies (
    PRIMARY KEY (agent_id) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    agent_id,
    window_time,
    COUNT(*)                                                   AS trace_count,
    AVG(loop_count)                                            AS avg_loops,
    SUM(CASE WHEN status <> 'success' THEN 1 ELSE 0 END)      AS error_count,
    is_anomaly,
    lower_bound,
    upper_bound
FROM TABLE(
    ML_DETECT_ANOMALIES(
        TABLE agent_traces,
        DESCRIPTOR(event_ts),
        DESCRIPTOR(loop_count),
        INTERVAL '5' MINUTE,
        'agent_id',
        'sensitivity' = '0.95',
        'minTrainingSize' = '10'
    )
)
WHERE is_anomaly = TRUE;


-- -----------------------------------------------------------------------------
-- STEP 3: Union both anomaly streams into a single ai_anomalies table
-- -----------------------------------------------------------------------------

CREATE TABLE ai_anomalies (
    PRIMARY KEY (entity, anomaly_type, window_time) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS
SELECT
    CONCAT(team, '/', feature_name) AS entity,
    'LLM_COST_SPIKE'                AS anomaly_type,
    window_time,
    CAST(total_cost_usd AS DOUBLE)  AS metric_value,
    CAST(upper_bound AS DOUBLE)     AS expected_upper,
    ROUND(((total_cost_usd - upper_bound) / upper_bound) * 100, 1) AS deviation_pct
FROM llm_cost_anomalies

UNION ALL

SELECT
    agent_id                        AS entity,
    'AGENT_LOOP_SPIKE'              AS anomaly_type,
    window_time,
    CAST(avg_loops AS DOUBLE)       AS metric_value,
    CAST(upper_bound AS DOUBLE)     AS expected_upper,
    ROUND(((avg_loops - upper_bound) / upper_bound) * 100, 1) AS deviation_pct
FROM agent_health_anomalies;


-- -----------------------------------------------------------------------------
-- STEP 4: RAG enrichment — embed anomaly, vector search, LLM explains root cause
-- -----------------------------------------------------------------------------

CREATE TABLE ai_anomalies_enriched (
    PRIMARY KEY (entity, anomaly_type, window_time) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    a.entity,
    a.anomaly_type,
    a.window_time,
    a.metric_value,
    a.deviation_pct,
    rad.top_documents,
    TRIM(ML_PREDICT(
        'llm_connection',
        CONCAT(
            'You are an AI operations expert. Analyze this anomaly and the retrieved context documents. ',
            'Identify the most likely root cause in 1-2 sentences. Be specific and actionable.\n\n',
            'ANOMALY: ', a.anomaly_type,
            ' detected for entity "', a.entity, '"',
            ' at ', CAST(a.window_time AS STRING),
            '. Metric value: ', CAST(a.metric_value AS STRING),
            ' (', CAST(a.deviation_pct AS STRING), '% above expected upper bound).\n\n',
            'RETRIEVED CONTEXT:\n', rad.top_documents
        )
    )) AS root_cause
FROM ai_anomalies a,
LATERAL (
    SELECT VECTOR_SEARCH_AGG(
        documents_vectordb_aiops,
        DESCRIPTOR(embedding),
        ML_PREDICT(
            'llm_embedding_connection',
            CONCAT(
                a.anomaly_type, ' anomaly for ', a.entity,
                '. Deviation: +', CAST(a.deviation_pct AS STRING), '%.',
                ' Window: ', CAST(a.window_time AS STRING)
            )
        ),
        3
    ) AS top_documents
) rad;


-- -----------------------------------------------------------------------------
-- STEP 5: Define the remediation tool and agent
-- (run these as standalone statements, not as CTAS)
-- -----------------------------------------------------------------------------

CREATE TOOL IF NOT EXISTS `aiops_remote_mcp`
WITH (
    'connection' = 'remote-mcp-connection'
);

CREATE AGENT IF NOT EXISTS `aiops_remediation_agent`
INPUT (entity STRING, anomaly_type STRING, root_cause STRING)
WITH (
    'tools' = 'aiops_remote_mcp',
    'model' = 'llm_connection',
    'instructions' = 'You are an AIOps remediation agent. You receive AI system anomaly alerts and take corrective action.

For LLM_COST_SPIKE anomalies:
1. Call http_get to fetch the current model routing config for the affected team/feature.
2. Identify if the team is using an oversized model (gpt-4 for a task that gpt-35-turbo can handle).
3. Call http_post to update the routing config, downgrading to the appropriate model.
4. Return a structured response with:
   Remediation Summary: [what you did and why]
   Config Change JSON: [the JSON payload you posted]
   API Response: [the response from the config API]

For AGENT_LOOP_SPIKE anomalies:
1. Call http_get to fetch the current status of the affected agent.
2. If loop_count > 10, call http_post to restart the agent with a fresh context.
3. Return a structured response with:
   Remediation Summary: [what you did and why]
   Action JSON: [the JSON payload you posted]
   API Response: [the response from the agent control API]'
);


-- -----------------------------------------------------------------------------
-- STEP 6: Autonomous remediation — run the agent, store results
-- -----------------------------------------------------------------------------

CREATE TABLE completed_actions (
    PRIMARY KEY (entity, anomaly_type) NOT ENFORCED
)
WITH ('changelog.mode' = 'append')
AS SELECT
    entity,
    anomaly_type,
    window_time,
    deviation_pct,
    root_cause,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        '(?:Remediation Summary|Action Summary):\s*\n([\s\S]+?)(?=\n\n(?:Config Change|Action) JSON:)', 1
    )) AS remediation_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        '(?:Config Change|Action) JSON:\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?(?=\n\nAPI Response:)', 1
    )) AS action_json,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        'API Response:\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?$', 1
    )) AS api_response,
    CAST(response AS STRING) AS raw_response
FROM ai_anomalies_enriched,
LATERAL TABLE(AI_RUN_AGENT(
    `aiops_remediation_agent`,
    `root_cause`,
    `entity`,
    `anomaly_type`
));
