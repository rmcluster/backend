package uiapi

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

type persistedConversation struct {
	ChatID    string             `json:"chat_id"`
	Title     string             `json:"title"`
	Model     string             `json:"model"`
	Status    string             `json:"status"`
	CreatedAt string             `json:"created_at"`
	UpdatedAt string             `json:"updated_at"`
	Messages  []persistedMessage `json:"messages"`
	Events    []chatEvent        `json:"events,omitempty"`
}

type persistedMessage struct {
	Sequence     int      `json:"sequence"`
	Role         string   `json:"role"`
	Content      string   `json:"content"`
	TokensPerSec *float64 `json:"tokens_per_sec,omitempty"`
	CreatedAt    string   `json:"created_at"`
}

type persistedRun struct {
	Snapshot                 chatRunSnapshot
	AssistantMessageSequence int
}

type chatStore struct {
	db *sql.DB
}

func newChatStore(db *sql.DB) *chatStore {
	return &chatStore{db: db}
}

func (s *chatStore) normalizeInterruptedRuns(ctx context.Context) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE runs
		 SET status = ?, error = ?, updated_at = ?, sequence = sequence + 1
		 WHERE status IN (?, ?, ?)`,
		string(chatRunStatusError),
		"stream interrupted by server restart",
		time.Now().UTC().Format(time.RFC3339),
		string(chatRunStatusWaiting),
		string(chatRunStatusStarting),
		string(chatRunStatusStreaming),
	)
	return err
}

func (s *chatStore) createConversation(ctx context.Context, chatID, model, startedAt string) error {
	if chatID == "" || model == "" {
		return fmt.Errorf("chat_id and model are required")
	}
	if startedAt == "" {
		startedAt = time.Now().UTC().Format(time.RFC3339)
	}
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO conversations (chat_id, title, model, status, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chat_id) DO NOTHING`,
		chatID,
		"New conversation",
		model,
		"active",
		startedAt,
		startedAt,
	)
	return err
}

func (s *chatStore) deleteConversation(ctx context.Context, chatID string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM conversations WHERE chat_id = ?`, chatID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *chatStore) listConversations(ctx context.Context) ([]persistedConversation, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT chat_id, title, model, status, created_at, updated_at
		   FROM conversations
		  ORDER BY updated_at DESC`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var conversations []persistedConversation
	for rows.Next() {
		var conv persistedConversation
		if err := rows.Scan(&conv.ChatID, &conv.Title, &conv.Model, &conv.Status, &conv.CreatedAt, &conv.UpdatedAt); err != nil {
			return nil, err
		}
		conversations = append(conversations, conv)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for i := range conversations {
		messages, err := s.listMessages(ctx, conversations[i].ChatID)
		if err != nil {
			return nil, err
		}
		conversations[i].Messages = messages
	}

	return conversations, nil
}

func (s *chatStore) getConversation(ctx context.Context, chatID string) (persistedConversation, error) {
	var conv persistedConversation
	err := s.db.QueryRowContext(
		ctx,
		`SELECT chat_id, title, model, status, created_at, updated_at
		   FROM conversations
		  WHERE chat_id = ?`,
		chatID,
	).Scan(&conv.ChatID, &conv.Title, &conv.Model, &conv.Status, &conv.CreatedAt, &conv.UpdatedAt)
	if err != nil {
		return persistedConversation{}, err
	}
	conv.Messages, err = s.listMessages(ctx, chatID)
	if err != nil {
		return persistedConversation{}, err
	}
	conv.Events, err = s.listEvents(ctx, chatID)
	if err != nil {
		return persistedConversation{}, err
	}
	return conv, nil
}

func (s *chatStore) listMessages(ctx context.Context, chatID string) ([]persistedMessage, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT sequence, role, content, tokens_per_sec, created_at
		   FROM messages
		  WHERE chat_id = ?
		  ORDER BY sequence ASC`,
		chatID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []persistedMessage
	for rows.Next() {
		var message persistedMessage
		if err := rows.Scan(&message.Sequence, &message.Role, &message.Content, &message.TokensPerSec, &message.CreatedAt); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}
	return messages, rows.Err()
}

func (s *chatStore) listEvents(ctx context.Context, chatID string) ([]chatEvent, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT sequence, event_type, message_id, role, content, token, error, timestamp
		   FROM events
		  WHERE chat_id = ?
		  ORDER BY sequence ASC`,
		chatID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []chatEvent
	for rows.Next() {
		var event chatEvent
		if err := rows.Scan(
			&event.Sequence,
			&event.EventType,
			&event.MessageID,
			&event.Role,
			&event.Content,
			&event.Token,
			&event.Error,
			&event.Timestamp,
		); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *chatStore) appendEvent(ctx context.Context, chatID string, req chatEventRequest) (chatEvent, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return chatEvent{}, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	seq, err := nextSequenceTx(ctx, tx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM events WHERE chat_id = ?`, chatID)
	if err != nil {
		return chatEvent{}, err
	}
	if _, err = tx.ExecContext(
		ctx,
		`INSERT INTO events (chat_id, sequence, event_type, message_id, role, content, token, error, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		chatID,
		seq,
		req.EventType,
		req.MessageID,
		req.Role,
		req.Content,
		req.Token,
		req.Error,
		req.Timestamp,
	); err != nil {
		return chatEvent{}, err
	}

	if _, err = tx.ExecContext(
		ctx,
		`UPDATE conversations SET updated_at = ? WHERE chat_id = ?`,
		req.Timestamp,
		chatID,
	); err != nil {
		return chatEvent{}, err
	}

	if err = tx.Commit(); err != nil {
		return chatEvent{}, err
	}
	return chatEvent{chatEventRequest: req, Sequence: seq}, nil
}

func (s *chatStore) appendUserMessage(ctx context.Context, chatID, content, createdAt string) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	seq, err := nextSequenceTx(ctx, tx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE chat_id = ?`, chatID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		ctx,
		`INSERT INTO messages (chat_id, sequence, role, content, tokens_per_sec, created_at)
		 VALUES (?, ?, ?, ?, NULL, ?)`,
		chatID,
		seq,
		"user",
		content,
		createdAt,
	); err != nil {
		return err
	}
	if _, err = tx.ExecContext(
		ctx,
		`UPDATE conversations
		    SET title = CASE WHEN title = 'New conversation' THEN ? ELSE title END,
		        updated_at = ?
		  WHERE chat_id = ?`,
		truncateTitle(content),
		createdAt,
		chatID,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *chatStore) createRun(ctx context.Context, snapshot chatRunSnapshot) (int, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	msgSeq, err := nextSequenceTx(ctx, tx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM messages WHERE chat_id = ?`, snapshot.ChatID)
	if err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(
		ctx,
		`INSERT INTO runs
		   (chat_id, model, status, assistant_content, assistant_message_sequence, loading_phase, loading_progress, layers_on_rpc, started_at, updated_at, error, sequence)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chat_id) DO UPDATE SET
		   model = excluded.model,
		   status = excluded.status,
		   assistant_content = excluded.assistant_content,
		   assistant_message_sequence = excluded.assistant_message_sequence,
		   loading_phase = excluded.loading_phase,
		   loading_progress = excluded.loading_progress,
		   layers_on_rpc = excluded.layers_on_rpc,
		   started_at = excluded.started_at,
		   updated_at = excluded.updated_at,
		   error = excluded.error,
		   sequence = excluded.sequence`,
		snapshot.ChatID,
		snapshot.Model,
		snapshot.Status,
		snapshot.AssistantContent,
		msgSeq,
		snapshot.LoadingPhase,
		snapshot.LoadingProgress,
		snapshot.LayersOnRPC,
		snapshot.StartedAt,
		snapshot.UpdatedAt,
		snapshot.Error,
		snapshot.Sequence,
	); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE chat_id = ? AND sequence = ?`, snapshot.ChatID, msgSeq); err != nil {
		return 0, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE conversations SET updated_at = ? WHERE chat_id = ?`, snapshot.UpdatedAt, snapshot.ChatID); err != nil {
		return 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return msgSeq, nil
}

func (s *chatStore) saveRunSnapshot(ctx context.Context, snapshot chatRunSnapshot, assistantMessageSequence int) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if _, err = tx.ExecContext(
		ctx,
		`INSERT INTO runs
		   (chat_id, model, status, assistant_content, assistant_message_sequence, loading_phase, loading_progress, layers_on_rpc, started_at, updated_at, error, sequence)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(chat_id) DO UPDATE SET
		   model = excluded.model,
		   status = excluded.status,
		   assistant_content = excluded.assistant_content,
		   assistant_message_sequence = excluded.assistant_message_sequence,
		   loading_phase = excluded.loading_phase,
		   loading_progress = excluded.loading_progress,
		   layers_on_rpc = excluded.layers_on_rpc,
		   started_at = excluded.started_at,
		   updated_at = excluded.updated_at,
		   error = excluded.error,
		   sequence = excluded.sequence`,
		snapshot.ChatID,
		snapshot.Model,
		snapshot.Status,
		snapshot.AssistantContent,
		assistantMessageSequence,
		snapshot.LoadingPhase,
		snapshot.LoadingProgress,
		snapshot.LayersOnRPC,
		snapshot.StartedAt,
		snapshot.UpdatedAt,
		snapshot.Error,
		snapshot.Sequence,
	); err != nil {
		return err
	}

	if _, err = tx.ExecContext(
		ctx,
		`INSERT INTO messages (chat_id, sequence, role, content, tokens_per_sec, created_at)
		 VALUES (?, ?, ?, ?, NULL, ?)
		 ON CONFLICT(chat_id, sequence) DO UPDATE SET
		   content = excluded.content`,
		snapshot.ChatID,
		assistantMessageSequence,
		"assistant",
		snapshot.AssistantContent,
		snapshot.UpdatedAt,
	); err != nil {
		return err
	}

	status := "active"
	if snapshot.Status == chatRunStatusCompleted || snapshot.Status == chatRunStatusStopped || snapshot.Status == chatRunStatusError {
		status = "closed"
	}
	if _, err = tx.ExecContext(
		ctx,
		`UPDATE conversations SET status = ?, updated_at = ? WHERE chat_id = ?`,
		status,
		snapshot.UpdatedAt,
		snapshot.ChatID,
	); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *chatStore) getRun(ctx context.Context, chatID string) (persistedRun, error) {
	var run persistedRun
	err := s.db.QueryRowContext(
		ctx,
		`SELECT chat_id, model, status, assistant_content, loading_phase, loading_progress, layers_on_rpc,
		        started_at, updated_at, error, sequence, assistant_message_sequence
		   FROM runs
		  WHERE chat_id = ?`,
		chatID,
	).Scan(
		&run.Snapshot.ChatID,
		&run.Snapshot.Model,
		&run.Snapshot.Status,
		&run.Snapshot.AssistantContent,
		&run.Snapshot.LoadingPhase,
		&run.Snapshot.LoadingProgress,
		&run.Snapshot.LayersOnRPC,
		&run.Snapshot.StartedAt,
		&run.Snapshot.UpdatedAt,
		&run.Snapshot.Error,
		&run.Snapshot.Sequence,
		&run.AssistantMessageSequence,
	)
	return run, err
}

func nextSequenceTx(ctx context.Context, tx *sql.Tx, query string, chatID string) (int, error) {
	var seq int
	err := tx.QueryRowContext(ctx, query, chatID).Scan(&seq)
	return seq, err
}

func truncateTitle(content string) string {
	content = strings.TrimSpace(content)
	if len(content) <= 40 {
		return content
	}
	return content[:40]
}

func isNotFoundError(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
