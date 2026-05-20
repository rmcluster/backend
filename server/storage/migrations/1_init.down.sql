DROP INDEX IF EXISTS idx_file_chunks_hash;
DROP INDEX IF EXISTS idx_files_parent;
DROP INDEX IF EXISTS idx_directories_parent;

DROP TABLE IF EXISTS chunk_refs;
DROP TABLE IF EXISTS file_chunks;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS directories;