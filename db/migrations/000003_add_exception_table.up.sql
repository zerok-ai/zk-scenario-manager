ALTER TABLE span
    ADD COLUMN errors TEXT;

CREATE TABLE IF NOT EXISTS errors_data
(
    id   varchar(100) NOT NULL PRIMARY KEY,
    data BYTEA
);