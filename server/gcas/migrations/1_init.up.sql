-- The external node id is stored in name as text.
-- Internally, nodes use integer ids
CREATE TABLE nodes {
    id integer PRIMARY KEY AUTOINCREMENT,
    name text UNIQUE
};

CREATE INDEX node_name_idx ON nodes(name);

CREATE TABLE chunks {
    hash binary(32) primary key,
    size int,
    node_id int REFERENCES nodes(id)
};