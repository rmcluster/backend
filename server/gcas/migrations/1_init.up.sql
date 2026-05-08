CREATE TABLE chunks (
    hash binary(32) primary key,
    size int,
    node_id text
);