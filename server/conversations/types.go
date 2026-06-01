package conversations

import "errors"

type Conversation struct {
	Id        string            `json:"id"`
	Object    string            `json:"object"` // should always be "conversation"
	CreatedAt int64             `json:"created_at"`
	Metadata  map[string]string `json:"metadata"`
}

var (
	ErrInvalidConversationId        = errors.New("invalid conversation id")
	ErrInvalidConversationObject    = errors.New("invalid conversation object")
	ErrInvalidConversationCreatedAt = errors.New("invalid conversation created_at")
	ErrConversationNotFound         = errors.New("conversation not found")
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
