package conversations

import (
	"encoding/json"
	"errors"
)

type Conversation struct {
	Id        string            `json:"id"`
	Object    string            `json:"object"` // should always be "conversation"
	CreatedAt int64             `json:"created_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type Response struct {
	Id           string            `json:"id"`
	Object       string            `json:"object"` // should always be "response"
	CreatedAt    int64             `json:"created_at"`
	Model        string            `json:"model"`
	Conversation string            `json:"conversation,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	Input        json.RawMessage   `json:"input,omitempty"`
	Output       json.RawMessage   `json:"output,omitempty"`
}

var (
	ErrInvalidConversationId        = errors.New("invalid conversation id")
	ErrInvalidConversationObject    = errors.New("invalid conversation object")
	ErrInvalidConversationCreatedAt = errors.New("invalid conversation created_at")
	ErrConversationNotFound         = errors.New("conversation not found")

	ErrInvalidResponseId        = errors.New("invalid response id")
	ErrInvalidResponseObject    = errors.New("invalid response object")
	ErrInvalidResponseCreatedAt = errors.New("invalid response created_at")
	ErrInvalidResponseModel     = errors.New("invalid response model")
	ErrResponseNotFound         = errors.New("response not found")
)

func (c *Conversation) Validate() error {
	if c.Id == "" {
		return ErrInvalidConversationId
	}
	if c.Object != "conversation" {
		return ErrInvalidConversationObject
	}
	if c.CreatedAt <= 0 {
		return ErrInvalidConversationCreatedAt
	}
	return nil
}

func (r *Response) Validate() error {
	if r.Id == "" {
		return ErrInvalidResponseId
	}
	if r.Object != "response" {
		return ErrInvalidResponseObject
	}
	if r.CreatedAt <= 0 {
		return ErrInvalidResponseCreatedAt
	}
	if r.Model == "" {
		return ErrInvalidResponseModel
	}
	return nil
}
