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

CREATE TABLE responses (
    id TEXT PRIMARY KEY,
    object TEXT NOT NULL,
    conversation_id TEXT,
    model TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    input TEXT,
    output TEXT,
    FOREIGN KEY (conversation_id) REFERENCES conversations (id) ON DELETE CASCADE
);

CREATE TABLE response_metadata (
    response_id TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (response_id, key),
    FOREIGN KEY (response_id) REFERENCES responses (id) ON DELETE CASCADE
);