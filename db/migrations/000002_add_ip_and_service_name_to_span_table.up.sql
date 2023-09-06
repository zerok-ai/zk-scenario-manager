ALTER TABLE span
    ADD COLUMN source_ip VARCHAR(30);

ALTER TABLE span
    ADD COLUMN destination_ip VARCHAR(30);

ALTER TABLE span
    ADD COLUMN service_name VARCHAR(255);
