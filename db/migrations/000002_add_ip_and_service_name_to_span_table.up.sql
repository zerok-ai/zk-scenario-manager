ALTER TABLE span
    ADD COLUMN source_ip VARCHAR(30);

-- Add the destination_ip column
ALTER TABLE span
    ADD COLUMN destination_ip VARCHAR(30);

-- Add the service_name column
ALTER TABLE span
    ADD COLUMN service_name VARCHAR(255);
