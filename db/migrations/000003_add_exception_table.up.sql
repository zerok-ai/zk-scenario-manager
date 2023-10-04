ALTER TABLE span
    ADD COLUMN errors VARCHAR(255);

CREATE TABLE IF NOT EXISTS errors_data
(
    id   varchar(100) NOT NULL PRIMARY KEY,
    data BYTEA
);