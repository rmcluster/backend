package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/openai/openai-go/v2"
	"github.com/openai/openai-go/v2/conversations"
	"github.com/openai/openai-go/v2/option"
)

func main() {
	baseURL := flag.String("base-url", "", "Base URL of the OpenAI-compatible conversations API. Example: http://127.0.0.1:4917/v1")
	flag.Parse()

	if *baseURL == "" {
		flag.Usage()
		os.Exit(2)
	}

	normalizedBaseURL, err := normalizeBaseURL(*baseURL)
	if err != nil {
		log.Fatalf("invalid base URL %q: %v", *baseURL, err)
	}

	log.Printf("Testing conversations API at %s", normalizedBaseURL)
	client := openai.NewClient(option.WithBaseURL(normalizedBaseURL))
	conversationSvc := conversations.NewConversationService(option.WithBaseURL(normalizedBaseURL))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log.Printf("Creating conversation")
	conversation, err := createConversation(ctx, conversationSvc)
	if err != nil {
		log.Fatalf("conversation create failed: %v", err)
	}
	log.Printf("Created conversation %s", conversation.ID)

	log.Printf("Fetching conversation %s", conversation.ID)
	fetched, err := conversationSvc.Get(ctx, conversation.ID)
	if err != nil {
		log.Fatalf("conversation get failed: %v", err)
	}
	if fetched.ID != conversation.ID {
		log.Fatalf("conversation ID mismatch: created %q, fetched %q", conversation.ID, fetched.ID)
	}
	log.Printf("Fetched conversation %s", fetched.ID)

	log.Printf("Listing conversations")
	list, err := listConversations(ctx, client)
	if err != nil {
		log.Fatalf("conversation list failed: %v", err)
	}
	if !containsConversation(list, conversation.ID) {
		log.Fatalf("conversation %q not found in list", conversation.ID)
	}
	log.Printf("Verified conversation %s appears in list", conversation.ID)

	log.Printf("Deleting conversation %s", conversation.ID)
	deleted, err := conversationSvc.Delete(ctx, conversation.ID)
	if err != nil {
		log.Fatalf("conversation delete failed: %v", err)
	}
	if !deleted.Deleted {
		log.Fatalf("conversation delete returned deleted=false for %q", conversation.ID)
	}
	log.Printf("Deleted conversation %s", conversation.ID)

	fmt.Printf("Conversation API smoke test succeeded against %s\n", normalizedBaseURL)
}

func normalizeBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}

	if !strings.HasSuffix(u.Path, "/v1") && !strings.HasSuffix(u.Path, "/v1/") {
		u.Path = strings.TrimRight(u.Path, "/") + "/v1"
	}

	return u.String(), nil
}

func createConversation(ctx context.Context, svc conversations.ConversationService) (*conversations.Conversation, error) {
	req := conversations.ConversationNewParams{
		Metadata: openai.Metadata{"source": "test_conversations"},
	}
	return svc.New(ctx, req)
}

func listConversations(ctx context.Context, client openai.Client) ([]conversations.Conversation, error) {
	var response struct {
		Object string                       `json:"object"`
		Data   []conversations.Conversation `json:"data"`
	}
	if err := client.Get(ctx, "conversations", nil, &response); err != nil {
		return nil, err
	}
	return response.Data, nil
}

func containsConversation(list []conversations.Conversation, conversationID string) bool {
	for _, item := range list {
		if item.ID == conversationID {
			return true
		}
	}
	return false
}
