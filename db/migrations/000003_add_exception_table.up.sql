ALTER TABLE span
    ADD COLUMN error_type     VARCHAR(255),
    ADD COLUMN error_table_id VARCHAR(255);

CREATE TABLE IF NOT EXISTS exception_data
(
    id             varchar(100) NOT NULL PRIMARY KEY,
    exception_body BYTEA
);