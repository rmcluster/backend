package conversations

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestCreateGetListResponse(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "conversations.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := NewConversationsService(db, "http://example.com")

	conversation := &Conversation{
		Id:        "conv_test",
		Object:    "conversation",
		CreatedAt: time.Now().Unix(),
		Metadata:  map[string]string{"topic": "testing"},
	}
	if err := svc.CreateConversation(conversation); err != nil {
		t.Fatal(err)
	}

	resp := &Response{
		Model:        "gpt-test",
		Conversation: "conv_test",
		Metadata:     map[string]string{"role": "assistant"},
		Input:        json.RawMessage(`{"content":"hello"}`),
		Output:       json.RawMessage(`{"content":"world"}`),
	}
	if err := svc.CreateResponse(resp); err != nil {
		t.Fatal(err)
	}
	if resp.Id == "" {
		t.Fatal("expected response id to be generated")
	}
	if resp.CreatedAt == 0 {
		t.Fatal("expected response created_at to be set")
	}

	fetched, err := svc.GetResponse(resp.Id)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Id != resp.Id {
		t.Fatalf("expected id %q, got %q", resp.Id, fetched.Id)
	}
	if fetched.Model != resp.Model {
		t.Fatalf("expected model %q, got %q", resp.Model, fetched.Model)
	}
	if fetched.Conversation != resp.Conversation {
		t.Fatalf("expected conversation %q, got %q", resp.Conversation, fetched.Conversation)
	}
	if string(fetched.Input) != string(resp.Input) {
		t.Fatalf("expected input %s, got %s", resp.Input, fetched.Input)
	}
	if string(fetched.Output) != string(resp.Output) {
		t.Fatalf("expected output %s, got %s", resp.Output, fetched.Output)
	}
	if fetched.Metadata["role"] != "assistant" {
		t.Fatalf("expected metadata role to be assistant, got %q", fetched.Metadata["role"])
	}

	responses, err := svc.ListResponses()
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Id != resp.Id {
		t.Fatalf("expected response id %q in list, got %q", resp.Id, responses[0].Id)
	}
}

func TestCreateGetListResponseWithoutConversation(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "conversations.db")
	db, err := OpenDB(dbPath, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	svc := NewConversationsService(db, "http://example.com")

	resp := &Response{
		Model:    "gpt-test",
		Metadata: map[string]string{"role": "assistant"},
		Input:    json.RawMessage(`{"content":"hello"}`),
		Output:   json.RawMessage(`{"content":"world"}`),
	}
	if err := svc.CreateResponse(resp); err != nil {
		t.Fatal(err)
	}

	fetched, err := svc.GetResponse(resp.Id)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Id != resp.Id {
		t.Fatalf("expected id %q, got %q", resp.Id, fetched.Id)
	}
	if fetched.Conversation != "" {
		t.Fatalf("expected empty conversation, got %q", fetched.Conversation)
	}

	responses, err := svc.ListResponses()
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 {
		t.Fatalf("expected 1 response, got %d", len(responses))
	}
	if responses[0].Id != resp.Id {
		t.Fatalf("expected response id %q in list, got %q", resp.Id, responses[0].Id)
	}
	if responses[0].Conversation != "" {
		t.Fatalf("expected empty conversation in list, got %q", responses[0].Conversation)
	}
}
