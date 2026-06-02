CREATE TABLE conversations (
    chat_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    model TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE messages (
    chat_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    tokens_per_sec REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (chat_id, sequence),
    FOREIGN KEY (chat_id) REFERENCES conversations(chat_id) ON DELETE CASCADE
);

CREATE TABLE events (
    chat_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    message_id TEXT,
    role TEXT,
    content TEXT,
    token TEXT,
    error TEXT,
    timestamp TEXT NOT NULL,
    PRIMARY KEY (chat_id, sequence),
    FOREIGN KEY (chat_id) REFERENCES conversations(chat_id) ON DELETE CASCADE
);

CREATE TABLE runs (
    chat_id TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    status TEXT NOT NULL,
    assistant_content TEXT NOT NULL,
    assistant_message_sequence INTEGER NOT NULL,
    loading_phase TEXT NOT NULL,
    loading_progress REAL NOT NULL,
    layers_on_rpc INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    error TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    FOREIGN KEY (chat_id) REFERENCES conversations(chat_id) ON DELETE CASCADE
);
