ALTER TABLE span
    ADD COLUMN span_attributes JSONB DEFAULT '';

ALTER TABLE span
    ADD COLUMN resource_attributes JSONB DEFAULT '';

ALTER TABLE span
    ADD COLUMN scope_attributes JSONB DEFAULT '';
