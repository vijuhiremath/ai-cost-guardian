-- =============================================================================
-- AIOps Monitoring Agent - Flink SQL Pipeline
-- Run ONE statement at a time in the Confluent Cloud Flink SQL workspace
-- =============================================================================


-- -----------------------------------------------------------------------------
-- STEP 1A: Create input table for LLM API calls
-- event_ts is epoch-ms from JSON; event_time is the derived watermarked column
-- -----------------------------------------------------------------------------

CREATE TABLE llm_api_calls (
    call_id STRING NOT NULL,
    team STRING NOT NULL,
    feature_name STRING NOT NULL,
    model STRING NOT NULL,
    input_tokens INT NOT NULL,
    output_tokens INT NOT NULL,
    cost_usd DOUBLE NOT NULL,
    latency_ms INT NOT NULL,
    quality_score DOUBLE NOT NULL,
    event_ts TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'scan.startup.mode' = 'latest-offset'
);


-- -----------------------------------------------------------------------------
-- STEP 1B: Create input table for agent execution traces
-- -----------------------------------------------------------------------------

CREATE TABLE agent_traces (
    trace_id STRING NOT NULL,
    agent_id STRING NOT NULL,
    tool_name STRING NOT NULL,
    status STRING NOT NULL,
    error_type STRING,
    loop_count INT NOT NULL,
    duration_ms INT NOT NULL,
    tokens_used INT NOT NULL,
    event_ts TIMESTAMP_LTZ(3) NOT NULL,
    WATERMARK FOR event_ts AS event_ts - INTERVAL '10' SECOND
) WITH (
    'scan.startup.mode' = 'latest-offset'
);


-- -----------------------------------------------------------------------------
-- STEP 2A: Detect LLM cost anomalies
-- 5-minute tumbling windows per team+feature, ML_DETECT_ANOMALIES on total cost
-- -----------------------------------------------------------------------------

CREATE TABLE llm_cost_anomalies
WITH ('changelog.mode' = 'append')
AS
WITH windowed_costs AS (
    SELECT
        window_start,
        window_end,
        window_time,
        team,
        feature_name,
        SUM(cost_usd)  AS total_cost_usd,
        COUNT(*)       AS call_count,
        AVG(cost_usd)  AS avg_cost_usd
    FROM TABLE(
        TUMBLE(TABLE llm_api_calls, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, team, feature_name
),
anomaly_detection AS (
    SELECT
        team,
        feature_name,
        window_time,
        total_cost_usd,
        call_count,
        avg_cost_usd,
        ML_DETECT_ANOMALIES(
            CAST(total_cost_usd AS DOUBLE),
            window_time,
            JSON_OBJECT(
                'minTrainingSize' VALUE 10,
                'maxTrainingSize' VALUE 7000,
                'confidencePercentage' VALUE 99.0,
                'enableStl' VALUE FALSE
            )
        ) OVER (
            PARTITION BY team, feature_name
            ORDER BY window_time
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly_result
    FROM windowed_costs
)
SELECT
    team,
    feature_name,
    window_time,
    total_cost_usd,
    call_count,
    anomaly_result.upper_bound  AS upper_bound,
    anomaly_result.lower_bound  AS lower_bound,
    anomaly_result.is_anomaly   AS is_anomaly
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = TRUE
  AND total_cost_usd > anomaly_result.upper_bound;


-- -----------------------------------------------------------------------------
-- STEP 2B: Detect agent health anomalies
-- 5-minute windows per agent_id, ML_DETECT_ANOMALIES on avg loop count
-- -----------------------------------------------------------------------------

CREATE TABLE agent_health_anomalies
WITH ('changelog.mode' = 'append')
AS
WITH windowed_traces AS (
    SELECT
        window_start,
        window_end,
        window_time,
        agent_id,
        AVG(CAST(loop_count AS DOUBLE))                              AS avg_loops,
        COUNT(*)                                                     AS trace_count,
        SUM(CASE WHEN status <> 'success' THEN 1 ELSE 0 END)        AS error_count
    FROM TABLE(
        TUMBLE(TABLE agent_traces, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
    )
    GROUP BY window_start, window_end, window_time, agent_id
),
anomaly_detection AS (
    SELECT
        agent_id,
        window_time,
        avg_loops,
        trace_count,
        error_count,
        ML_DETECT_ANOMALIES(
            avg_loops,
            window_time,
            JSON_OBJECT(
                'minTrainingSize' VALUE 10,
                'maxTrainingSize' VALUE 7000,
                'confidencePercentage' VALUE 99.0,
                'enableStl' VALUE FALSE
            )
        ) OVER (
            PARTITION BY agent_id
            ORDER BY window_time
            RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly_result
    FROM windowed_traces
)
SELECT
    agent_id,
    window_time,
    avg_loops,
    trace_count,
    error_count,
    anomaly_result.upper_bound  AS upper_bound,
    anomaly_result.lower_bound  AS lower_bound,
    anomaly_result.is_anomaly   AS is_anomaly
FROM anomaly_detection
WHERE anomaly_result.is_anomaly = TRUE
  AND avg_loops > anomaly_result.upper_bound;


-- -----------------------------------------------------------------------------
-- STEP 3: Union both anomaly streams into a single ai_anomalies table
-- -----------------------------------------------------------------------------

CREATE TABLE ai_anomalies
WITH ('changelog.mode' = 'append')
AS
SELECT
    CONCAT(team, '/', feature_name)                                     AS entity,
    'LLM_COST_SPIKE'                                                    AS anomaly_type,
    window_time,
    total_cost_usd                                                      AS metric_value,
    upper_bound                                                         AS expected_upper,
    ROUND(((total_cost_usd - upper_bound) / upper_bound) * 100, 1)     AS deviation_pct
FROM llm_cost_anomalies

UNION ALL

SELECT
    agent_id                                                            AS entity,
    'AGENT_LOOP_SPIKE'                                                  AS anomaly_type,
    window_time,
    avg_loops                                                           AS metric_value,
    upper_bound                                                         AS expected_upper,
    ROUND(((avg_loops - upper_bound) / upper_bound) * 100, 1)          AS deviation_pct
FROM agent_health_anomalies;


-- -----------------------------------------------------------------------------
-- STEP 4: RAG enrichment — embed anomaly, vector search, LLM explains root cause
-- -----------------------------------------------------------------------------

CREATE TABLE ai_anomalies_enriched
WITH ('changelog.mode' = 'append')
AS SELECT
    a.entity,
    a.anomaly_type,
    a.window_time,
    a.metric_value,
    a.deviation_pct,
    rad_with_rag.top_chunk_1,
    rad_with_rag.top_chunk_2,
    rad_with_rag.top_chunk_3,
    TRIM(llm_response.response) AS root_cause
FROM ai_anomalies a,
LATERAL TABLE(ML_PREDICT(
    'llm_embedding_model',
    CONCAT(
        a.anomaly_type, ' detected for ', a.entity,
        ' at ', CAST(a.window_time AS STRING),
        '. Metric: ', CAST(a.metric_value AS STRING),
        ' (+', CAST(a.deviation_pct AS STRING), '% above expected).'
    )
)) AS emb,
LATERAL TABLE(
    VECTOR_SEARCH_AGG(
        documents_vectordb_aiops,
        DESCRIPTOR(embedding),
        emb.embedding,
        3
    )
) AS vs,
LATERAL TABLE(
    ML_PREDICT(
        'llm_textgen_model',
        CONCAT(
            'You are an AI operations expert. Identify the root cause of this anomaly in 1-2 sentences and suggest a specific remediation action.\n\n',
            'ANOMALY: ', a.anomaly_type, ' for entity "', a.entity, '"',
            ' at ', CAST(a.window_time AS STRING),
            '. Value: ', CAST(a.metric_value AS STRING),
            ' (+', CAST(a.deviation_pct AS STRING), '% above expected).\n\n',
            'CONTEXT:\n',
            vs.search_results[1].chunk, '\n',
            vs.search_results[2].chunk, '\n',
            vs.search_results[3].chunk
        )
    )
) AS llm_response,
LATERAL (
    SELECT
        vs.search_results[1].chunk AS top_chunk_1,
        vs.search_results[2].chunk AS top_chunk_2,
        vs.search_results[3].chunk AS top_chunk_3
) AS rad_with_rag;


-- -----------------------------------------------------------------------------
-- STEP 5A: Define the remote MCP tool
-- (run as a standalone statement)
-- -----------------------------------------------------------------------------

CREATE TOOL aiops_remote_mcp
USING CONNECTION `remote-mcp-connection`
WITH (
    'type' = 'mcp',
    'allowed_tools' = 'http_get, http_post',
    'request_timeout' = '30'
);


-- -----------------------------------------------------------------------------
-- STEP 5B: Define the remediation agent
-- (run as a standalone statement)
-- -----------------------------------------------------------------------------

CREATE AGENT `aiops_remediation_agent`
USING MODEL `remote_mcp_model`
USING PROMPT 'You are an AIOps remediation agent. You receive real-time AI system anomaly alerts and take corrective action autonomously.

For LLM_COST_SPIKE anomalies:
1. Use http_get to fetch the current model routing config: https://p8jrtzaj78.execute-api.us-east-1.amazonaws.com/prod/api/model_routing
2. Identify if the team is using an oversized model (e.g. gpt-4 for a task gpt-35-turbo handles).
3. Use http_post to update the routing config, downgrading to the cheaper model.
4. Format your response with exactly these three sections:

Remediation Summary:
[What you changed and why]

Action JSON:
[The JSON payload you posted]

API Response:
[The response from the API]

For AGENT_LOOP_SPIKE anomalies:
1. Use http_get to fetch the current agent status: https://p8jrtzaj78.execute-api.us-east-1.amazonaws.com/prod/api/agent_status
2. If loop_count is high, use http_post to restart the agent with a fresh context.
3. Format your response with exactly these three sections:

Remediation Summary:
[What you did and why]

Action JSON:
[The JSON payload you posted]

API Response:
[The response from the API]

CRITICAL: Always execute the API calls. Never ask for clarification. Always include all three labeled sections.'
USING TOOLS `aiops_remote_mcp`
WITH (
    'max_iterations' = '10'
);


-- -----------------------------------------------------------------------------
-- STEP 6: Autonomous remediation — run the agent, store results
-- -----------------------------------------------------------------------------

CREATE TABLE completed_actions
WITH ('changelog.mode' = 'append')
AS SELECT
    entity,
    anomaly_type,
    window_time,
    deviation_pct,
    root_cause,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        'Remediation Summary:\s*\n([\s\S]+?)(?=\n\nAction JSON:)', 1
    )) AS remediation_summary,
    TRIM(REGEXP_EXTRACT(CAST(response AS STRING),
        'Action JSON:\s*\n(?:```json\s*)?([\s\S]+?)(?:```)?(?=\n\nAPI Response:)', 1
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
