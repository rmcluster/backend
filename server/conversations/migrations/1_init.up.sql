CREATE TABLE conversations (
    id TEXT PRIMARY KEY,
    object TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE conversation_metadata (
    conversation_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (conversation_id, key),
    FOREIGN KEY (conversation_id) REFERENCES conversations (id) ON DELETE CASCADE
);