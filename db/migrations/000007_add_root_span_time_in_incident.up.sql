ALTER TABLE incident ADD COLUMN root_span_time TIMESTAMP DEFAULT null;

UPDATE incident SET root_span_time = incident_collection_time WHERE root_span_time IS NULL;