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
    root_span_time           TIMESTAMP    NOT NULL,
    latency_ns               FLOAT,
    CONSTRAINT unique_issue  UNIQUE (issue_hash, trace_id)
);


CREATE TABLE IF NOT EXISTS span
(
    id               SERIAL PRIMARY KEY,
    trace_id         VARCHAR(255),
    span_id          VARCHAR(255),
    parent_span_id   VARCHAR(255),
    source           VARCHAR(255),
    destination      VARCHAR(255),
    workload_id_list TEXT[],
    status           VARCHAR(255),
    metadata         TEXT,
    latency_ns       FLOAT,
    protocol         VARCHAR(255),
    issue_hash_list  TEXT[],
    time             TIMESTAMP,
    CONSTRAINT unique_span UNIQUE (trace_id, span_id)
);


CREATE TABLE IF NOT EXISTS span_raw_data
(
    id               SERIAL PRIMARY KEY,
    trace_id         VARCHAR(255),
    span_id          VARCHAR(255),
    request_payload  BYTEA,
    response_payload BYTEA,
    CONSTRAINT unique_span_raw_data UNIQUE (trace_id, span_id)
);