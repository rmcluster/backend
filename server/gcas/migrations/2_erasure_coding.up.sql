-- add column to chunks to indicate if the chunk is part of the data
ALTER TABLE chunks
ADD COLUMN is_data BOOLEAN DEFAULT 1;

-- table of erasure coding groups
CREATE TABLE erasure_group (
    id INTEGER PRIMARY KEY
);

-- map data chunks to their erasure group ids
CREATE TABLE erasure_group_member (
    hash_id BLOB(32) PRIMARY KEY,
    erasure_group_id INTEGER,
    slice_idx int,
    FOREIGN KEY (erasure_group_id) REFERENCES erasure_group(id)
);