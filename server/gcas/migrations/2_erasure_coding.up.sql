-- add column to chunks to indicate if the chunk is part of the data
ALTER TABLE chunks
ADD COLUMN is_data BOOLEAN DEFAULT 1;

-- table of erasure coding groups
CREATE TABLE erasure_group (
    id            INTEGER PRIMARY KEY,
    data_shards   INTEGER NOT NULL,
    parity_shards INTEGER NOT NULL DEFAULT 2,
    shard_size    INTEGER NOT NULL  -- max chunk size in stripe (bytes), used for padding
);

-- map data and parity chunks to their erasure group
-- slice_idx: 0..data_shards-1 = data chunks, data_shards..data_shards+parity_shards-1 = parity
CREATE TABLE erasure_group_member (
    hash_id          BLOB(32) PRIMARY KEY,
    erasure_group_id INTEGER NOT NULL,
    slice_idx        INTEGER NOT NULL,
    FOREIGN KEY (erasure_group_id) REFERENCES erasure_group(id)
);