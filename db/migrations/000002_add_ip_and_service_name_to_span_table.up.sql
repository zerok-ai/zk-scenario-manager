ALTER TABLE span
    ADD COLUMN source_ip VARCHAR(255);

ALTER TABLE span
    ADD COLUMN destination_ip VARCHAR(255);

ALTER TABLE span
    ADD COLUMN service_name VARCHAR(255);
