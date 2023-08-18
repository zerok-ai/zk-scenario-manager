CREATE TABLE IF NOT EXISTS issue
(
    id               SERIAL PRIMARY KEY,
    issue_hash       VARCHAR(255) UNIQUE,
    issue_title      VARCHAR(255),
    scenario_id      VARCHAR(255),
    scenario_version VARCHAR(255)
);


CREATE TABLE IF NOT EXISTS incident
(
    id                       SERIAL PRIMARY KEY,
    trace_id                 VARCHAR(255) NOT NULL,
    issue_hash               VARCHAR(255) NOT NULL,
    incident_collection_time TIMESTAMP    NOT NULL,
    entry_service            VARCHAR(255),
    end_point                VARCHAR(255),
    protocol                 VARCHAR(255),
    root_span_time           TIMESTAMP,
    latency_ns               FLOAT,
    CONSTRAINT unique_issue UNIQUE (issue_hash, trace_id)
);


CREATE TABLE IF NOT EXISTS span
(
    id                    SERIAL PRIMARY KEY,
    trace_id              VARCHAR(255),
    parent_span_id        VARCHAR(255),
    span_id               VARCHAR(255),
    is_root               BOOLEAN,
    kind                  VARCHAR(255),
    start_time            TIMESTAMP,
    latency               DOUBLE PRECISION,
    source                VARCHAR(255),
    destination           VARCHAR(255),
    workload_id_list      TEXT[],
    protocol              VARCHAR(255),
    issue_hash_list       TEXT[],
    request_payload_size  BIGINT,
    response_payload_size BIGINT,
    method                VARCHAR(255),
    route                 VARCHAR(255),
    scheme                VARCHAR(255),
    path                  VARCHAR(255),
    query                 VARCHAR(255),
    status                INTEGER,
    metadata              TEXT,
    username              VARCHAR(255),
    CONSTRAINT unique_trace_span_id UNIQUE (trace_id, span_id)
);

CREATE TABLE span_raw_data
(
    id           SERIAL PRIMARY KEY,
    trace_id     VARCHAR(255),
    span_id      VARCHAR(255),
    req_headers  TEXT,
    resp_headers TEXT,
    is_truncated BOOLEAN,
    req_body     BYTEA,
    resp_body    BYTEA,
    CONSTRAINT unique_span_trace UNIQUE (span_id, trace_id)
);