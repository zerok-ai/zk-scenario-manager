ALTER TABLE span
    ADD COLUMN span_attributes JSONB DEFAULT '{}'::jsonb;

ALTER TABLE span
    ADD COLUMN resource_attributes JSONB DEFAULT '{}'::jsonb;

ALTER TABLE span
    ADD COLUMN scope_attributes JSONB DEFAULT '{}'::jsonb;

ALTER TABLE span
    ALTER COLUMN latency SET DATA TYPE BIGINT;