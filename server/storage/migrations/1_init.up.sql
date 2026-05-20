CREATE TABLE directories (
    path          TEXT    PRIMARY KEY,
    parent_path   TEXT    NOT NULL,
    mode          INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL
);

CREATE TABLE files (
    path        TEXT    PRIMARY KEY,
    parent_path TEXT    NOT NULL,
    mode        INTEGER NOT NULL,
    size_bytes  INTEGER NOT NULL,
    mod_time_ns INTEGER NOT NULL,
    version     INTEGER NOT NULL
);

CREATE TABLE file_chunks (
    file_path   TEXT    NOT NULL,
    chunk_index INTEGER NOT NULL,
    chunk_hash  BLOB    NOT NULL,
    chunk_size  INTEGER NOT NULL,
    PRIMARY KEY (file_path, chunk_index)
);

CREATE TABLE chunk_refs (
    chunk_hash BLOB    PRIMARY KEY,
    ref_count  INTEGER NOT NULL
);

CREATE INDEX idx_directories_parent ON directories(parent_path);
CREATE INDEX idx_files_parent       ON files(parent_path);
CREATE INDEX idx_file_chunks_hash   ON file_chunks(chunk_hash);

INSERT INTO directories(path, parent_path, mode, created_at_ns, updated_at_ns)
VALUES('/', '/', 493, 0, 0);