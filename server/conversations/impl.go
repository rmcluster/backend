package conversations

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// ConversationsService implements the Conversations and Responses OpenAI API endpoints,
// backed by a SQLite database and wrapping an OpenAI-compatible completions API.
type ConversationsService struct {
	db            *sql.DB
	openAIBaseUrl string // openAI endpoint providing completions API
}

func NewConversationsService(db *sql.DB, openAIBaseUrl string) *ConversationsService {
	return &ConversationsService{db: db, openAIBaseUrl: openAIBaseUrl}
}

func (s *ConversationsService) CreateConversation(conv *Conversation) error {
	if conv.Id == "" {
		conv.Id = generateConversationID()
	}
	if conv.Object == "" {
		conv.Object = "conversation"
	}
	if conv.CreatedAt == 0 {
		conv.CreatedAt = time.Now().Unix()
	}
	if conv.Metadata == nil {
		conv.Metadata = map[string]string{}
	}

	if err := conv.Validate(); err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`INSERT INTO conversations (id, object, created_at) VALUES (?, ?, ?)`, conv.Id, conv.Object, conv.CreatedAt); err != nil {
		return err
	}

	for key, value := range conv.Metadata {
		if _, err = tx.Exec(`INSERT INTO conversation_metadata (conversation_id, key, value) VALUES (?, ?, ?)`, conv.Id, key, value); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *ConversationsService) GetConversation(id string) (*Conversation, error) {
	if id == "" {
		return nil, ErrInvalidConversationId
	}

	rows, err := s.db.Query(`
SELECT c.id, c.object, c.created_at, m.key, m.value
FROM conversations c
LEFT JOIN conversation_metadata m ON c.id = m.conversation_id
WHERE c.id = ?
ORDER BY m.key ASC
`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var conversation *Conversation
	for rows.Next() {
		var cid, object string
		var createdAt int64
		var key sql.NullString
		var value sql.NullString
		if err := rows.Scan(&cid, &object, &createdAt, &key, &value); err != nil {
			return nil, err
		}
		if conversation == nil {
			conversation = &Conversation{
				Id:        cid,
				Object:    object,
				CreatedAt: createdAt,
				Metadata:  map[string]string{},
			}
		}
		if key.Valid {
			conversation.Metadata[key.String] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if conversation == nil {
		return nil, ErrConversationNotFound
	}

	return conversation, nil
}

func (s *ConversationsService) ListConversations() ([]Conversation, error) {
	rows, err := s.db.Query(`
SELECT c.id, c.object, c.created_at, m.key, m.value
FROM conversations c
LEFT JOIN conversation_metadata m ON c.id = m.conversation_id
ORDER BY c.created_at ASC, c.id ASC, m.key ASC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	conversationsMap := map[string]*Conversation{}
	order := []string{}

	for rows.Next() {
		var cid, object string
		var createdAt int64
		var key sql.NullString
		var value sql.NullString
		if err := rows.Scan(&cid, &object, &createdAt, &key, &value); err != nil {
			return nil, err
		}
		conversation, exists := conversationsMap[cid]
		if !exists {
			conversation = &Conversation{
				Id:        cid,
				Object:    object,
				CreatedAt: createdAt,
				Metadata:  map[string]string{},
			}
			conversationsMap[cid] = conversation
			order = append(order, cid)
		}
		if key.Valid {
			conversation.Metadata[key.String] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]Conversation, 0, len(order))
	for _, id := range order {
		result = append(result, *conversationsMap[id])
	}
	return result, nil
}

func (s *ConversationsService) DeleteConversation(id string) error {
	if id == "" {
		return ErrInvalidConversationId
	}

	result, err := s.db.Exec(`DELETE FROM conversations WHERE id = ?`, id)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrConversationNotFound
	}

	return nil
}

func (s *ConversationsService) CreateResponse(resp *Response) error {
	if resp.Id == "" {
		resp.Id = generateResponseID()
	}
	if resp.Object == "" {
		resp.Object = "response"
	}
	if resp.CreatedAt == 0 {
		resp.CreatedAt = time.Now().Unix()
	}
	if resp.Metadata == nil {
		resp.Metadata = map[string]string{}
	}

	if err := resp.Validate(); err != nil {
		return err
	}

	inputValue := interface{}(nil)
	if len(resp.Input) > 0 {
		inputValue = string(resp.Input)
	}
	outputValue := interface{}(nil)
	if len(resp.Output) > 0 {
		outputValue = string(resp.Output)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	if _, err = tx.Exec(`INSERT INTO responses (id, object, conversation_id, model, created_at, input, output) VALUES (?, ?, ?, ?, ?, ?, ?)`, resp.Id, resp.Object, nullableString(resp.Conversation), resp.Model, resp.CreatedAt, inputValue, outputValue); err != nil {
		return err
	}

	for key, value := range resp.Metadata {
		if _, err = tx.Exec(`INSERT INTO response_metadata (response_id, key, value) VALUES (?, ?, ?)`, resp.Id, key, value); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *ConversationsService) GetResponse(id string) (*Response, error) {
	if id == "" {
		return nil, ErrInvalidResponseId
	}

	rows, err := s.db.Query(`
SELECT r.id, r.object, r.conversation_id, r.model, r.created_at, r.input, r.output, m.key, m.value
FROM responses r
LEFT JOIN response_metadata m ON r.id = m.response_id
WHERE r.id = ?
ORDER BY m.key ASC
`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var response *Response
	for rows.Next() {
		var rid, object, conversationID, model string
		var createdAt int64
		var input sql.NullString
		var output sql.NullString
		var key sql.NullString
		var value sql.NullString
		if err := rows.Scan(&rid, &object, &conversationID, &model, &createdAt, &input, &output, &key, &value); err != nil {
			return nil, err
		}
		if response == nil {
			response = &Response{
				Id:           rid,
				Object:       object,
				Conversation: conversationID,
				Model:        model,
				CreatedAt:    createdAt,
				Metadata:     map[string]string{},
			}
			if input.Valid {
				response.Input = json.RawMessage(input.String)
			}
			if output.Valid {
				response.Output = json.RawMessage(output.String)
			}
		}
		if key.Valid {
			response.Metadata[key.String] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if response == nil {
		return nil, ErrResponseNotFound
	}

	return response, nil
}

func (s *ConversationsService) ListResponses() ([]Response, error) {
	rows, err := s.db.Query(`
SELECT r.id, r.object, r.conversation_id, r.model, r.created_at, r.input, r.output, m.key, m.value
FROM responses r
LEFT JOIN response_metadata m ON r.id = m.response_id
ORDER BY r.created_at ASC, r.id ASC, m.key ASC
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	responsesMap := map[string]*Response{}
	order := []string{}

	for rows.Next() {
		var rid, object, conversationID, model string
		var createdAt int64
		var input sql.NullString
		var output sql.NullString
		var key sql.NullString
		var value sql.NullString
		if err := rows.Scan(&rid, &object, &conversationID, &model, &createdAt, &input, &output, &key, &value); err != nil {
			return nil, err
		}
		resp, exists := responsesMap[rid]
		if !exists {
			resp = &Response{
				Id:           rid,
				Object:       object,
				Conversation: conversationID,
				Model:        model,
				CreatedAt:    createdAt,
				Metadata:     map[string]string{},
			}
			if input.Valid {
				resp.Input = json.RawMessage(input.String)
			}
			if output.Valid {
				resp.Output = json.RawMessage(output.String)
			}
			responsesMap[rid] = resp
			order = append(order, rid)
		}
		if key.Valid {
			resp.Metadata[key.String] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([]Response, 0, len(order))
	for _, id := range order {
		result = append(result, *responsesMap[id])
	}
	return result, nil
}

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func generateConversationID() string {
	var randomBytes [12]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return fmt.Sprintf("conv_%d", time.Now().UnixNano())
	}
	return "conv_" + hex.EncodeToString(randomBytes[:])
}

func generateResponseID() string {
	var randomBytes [12]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return fmt.Sprintf("resp_%d", time.Now().UnixNano())
	}
	return "resp_" + hex.EncodeToString(randomBytes[:])
}
