-- drop erasure coding tables
DROP TABLE erasure_group_member;
DROP TABLE erasure_group;

-- remove chunks that are not data
DELETE FROM chunks WHERE is_data = 0;

-- remove is_data column from chunks
ALTER TABLE chunks DROP COLUMN is_data;