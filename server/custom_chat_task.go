package server

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/openai/openai-go/v2"
	"github.com/rmcluster/backend/server/scheduling"
)

type customChatTask struct {
	model string
	w     http.ResponseWriter
	r     *http.Request
}

type customChatStatusEvent struct {
	Type       string  `json:"type"`
	Phase      string  `json:"phase"`
	Percentage float64 `json:"percentage"`
}

type customChatTokenEvent struct {
	Type  string `json:"type"`
	Token string `json:"token"`
}

type customChatNodesEvent struct {
	Type  string       `json:"type"`
	Nodes []nodeJsoner `json:"nodes"`
}

type nodeJsoner struct {
	node scheduling.Node
}

var _ json.Marshaler = nodeJsoner{}

func zeroNaN64(f float64) float64 {
	if math.IsNaN(f) {
		return 0
	}
	return f
}

func (n nodeJsoner) MarshalJSON() ([]byte, error) {
	// id is not included, because the real id isn't in the scheduler's Node interface,
	// and in any case would allow for impersonating nodes
	return json.Marshal(map[string]any{
		"ip":             n.node.Ip(),
		"port":           n.node.Port(),
		"storage_port":   n.node.StoragePort(),
		"max_size":       n.node.MaxSize(),
		"nickname":       n.node.Nickname(),
		"hardware_model": n.node.HardwareModel(),
		"battery":        zeroNaN64(n.node.Battery()),
		"temperature":    zeroNaN64(n.node.Temperature()),
	})
}

func newCustomChatTask(model string, w http.ResponseWriter, r *http.Request) *customChatTask {
	return &customChatTask{
		model: model,
		w:     w,
		r:     r,
	}
}

// Model implements [scheduling.Task].
func (p *customChatTask) Model() string {
	return p.model
}

// Fail writes a 503 error response so the waiting HTTP handler can unblock.
func (p *customChatTask) Fail(err error) {
	p.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	p.w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(p.w).Encode(map[string]string{"error": err.Error()})
}

// PerformInference implements [scheduling.Task].
func (p *customChatTask) PerformInference(instance scheduling.Instance) (err error) {
	// This API takes the same request body as the regular completions endpoint, but returns both tokens and status events as JSONL lines.

	// type for request body parsing
	type requestBody struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}

	// parse the request body to extract the model and messages
	var body requestBody
	err = json.NewDecoder(p.r.Body).Decode(&body)
	if err != nil {
		return err
	}

	// log the request for debugging
	log.Printf("Custom chat completion requested for model %s with %d messages", body.Model, len(body.Messages))

	// forward to the instance's OpenAI Client
	client := instance.GetOpenAIClient()
	completion := client.Chat.Completions.NewStreaming(p.r.Context(), openai.ChatCompletionNewParams{
		Model: body.Model,
		Messages: func() []openai.ChatCompletionMessageParamUnion {
			messages := make([]openai.ChatCompletionMessageParamUnion, len(body.Messages))
			for i, msg := range body.Messages {
				switch msg.Role {
				case "system":
					messages[i] = openai.SystemMessage(msg.Content)
				case "user":
					messages[i] = openai.UserMessage(msg.Content)
				case "assistant":
					messages[i] = openai.AssistantMessage(msg.Content)
				case "developer":
					messages[i] = openai.DeveloperMessage(msg.Content)
				default:
					log.Printf("Unknown message role %s, treating as user", msg.Role)
					messages[i] = openai.UserMessage(msg.Content)
				}
			}
			return messages
		}(),
	})
	if err != nil {
		return err
	}

	// lock to hold when writing to the response, to prevent interleaving of JSONL lines
	var writeLock sync.Mutex

	sendEvent := func(event any) error {
		writeLock.Lock()
		defer writeLock.Unlock()
		err := json.NewEncoder(p.w).Encode(event)
		if err != nil {
			return err
		} else {
			if f, ok := p.w.(http.Flusher); ok {
				f.Flush()
			}
		}
		return nil
	}

	// send initial nodes event
	err = sendEvent(customChatNodesEvent{
		Type: "nodes",
		Nodes: func() []nodeJsoner {
			nodes := instance.GetUsedNodes()
			jsoners := make([]nodeJsoner, len(nodes))
			for i, node := range nodes {
				jsoners[i] = nodeJsoner{node: node}
			}
			return jsoners
		}(),
	})
	if err != nil {
		log.Printf("Failed to send initial nodes event: %v", err)
	}

	// send a status event for the start of the completion
	err = sendEvent(customChatStatusEvent{
		Type:       "status",
		Phase:      "started",
		Percentage: 0,
	})
	if err != nil {
		log.Printf("Failed to send initial status event: %v", err)
	}

	var routinesDone sync.WaitGroup
	routinesDone.Add(1)
	stopStatusPolling := make(chan struct{})

	// start a goroutine to poll the completion status and send status events on changes
	go func() {
		defer routinesDone.Done()
		lastStatus := "started"
		lastPercentage := 0.0
		for {
			_, status, percentage, _ := instance.GetLoadingStatus()
			if status != lastStatus || percentage != lastPercentage {
				err := sendEvent(customChatStatusEvent{
					Type:       "status",
					Phase:      status,
					Percentage: percentage,
				})
				if err != nil {
					log.Printf("Failed to send status event: %v", err)
				}

				lastStatus = status
				lastPercentage = percentage
			}
			select {
			case <-p.r.Context().Done():
				return
			case <-stopStatusPolling:
				return
			case <-time.After(100 * time.Millisecond): // arbitrarily chosen polling interval
			}
		}
	}()

	// stream the response as JSONL lines
	for completion.Next() {
		chunk := completion.Current()
		if len(chunk.Choices) == 0 {
			continue
		}

		token := chunk.Choices[0].Delta.Content

		if token == "" {
			continue // don't forward empty tokens
		}

		err := sendEvent(customChatTokenEvent{
			Type:  "token",
			Token: token,
		})
		if err != nil {
			log.Printf("Failed to send token event: %v", err)
		}

		// flush the response so the client receives the chunk immediately
		if f, ok := p.w.(http.Flusher); ok {
			f.Flush()
		}
	}

	// wait for the status polling goroutine to finish
	close(stopStatusPolling)
	routinesDone.Wait()

	// send a final status event for the end of the completion
	sendEvent(customChatStatusEvent{
		Type:       "status",
		Phase:      "finished",
		Percentage: 100,
	})
	return completion.Err()
}

var _ scheduling.Task = (*customChatTask)(nil)
