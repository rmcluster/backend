CREATE TABLE device_metadata (
    node_id         TEXT    PRIMARY KEY,
    display_name    TEXT    NOT NULL DEFAULT '',
    updated_at_ns   INTEGER NOT NULL
);
