package metrics

import (
	"sort"
	"sync"
	"time"
)

type RequestMetric struct {
	ID                 int64    `json:"id"`
	ClientRequestID    string   `json:"client_request_id"`
	Model              string   `json:"model"`
	Path               string   `json:"path"`
	StartedAt          string   `json:"started_at"`
	CompletedAt        string   `json:"completed_at"`
	DurationMs         int64    `json:"duration_ms"`
	ResponseBytes      int64    `json:"response_bytes"`
	StreamedTextBytes  int64    `json:"streamed_text_bytes"`
	TokensStreamed     int64    `json:"tokens_streamed"`
	TokensPerSecond    float64  `json:"tokens_per_second"`
	AllocatedNodeCount int      `json:"allocated_node_count"`
	AllocatedNodeIDs   []string `json:"allocated_node_ids"`
}

type Summary struct {
	RequestCount       int     `json:"request_count"`
	AvgDurationMs      float64 `json:"avg_duration_ms"`
	AvgTokensPerSec    float64 `json:"avg_tokens_per_second"`
	TotalResponseBytes int64   `json:"total_response_bytes"`
	TotalTokens        int64   `json:"total_tokens_streamed"`
}

type Snapshot struct {
	Summary  Summary         `json:"summary"`
	Requests []RequestMetric `json:"requests"`
}

type Collector struct {
	mu       sync.Mutex
	nextID   int64
	maxItems int
	items    []RequestMetric
}

func NewCollector(maxItems int) *Collector {
	if maxItems < 1 {
		maxItems = 1
	}
	return &Collector{maxItems: maxItems}
}

func (c *Collector) Record(metric RequestMetric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextID++
	metric.ID = c.nextID
	if metric.CompletedAt == "" {
		metric.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}

	c.items = append(c.items, metric)
	if len(c.items) > c.maxItems {
		c.items = append([]RequestMetric(nil), c.items[len(c.items)-c.maxItems:]...)
	}
}

func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	items := append([]RequestMetric(nil), c.items...)
	c.mu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].ID > items[j].ID
	})

	var summary Summary
	summary.RequestCount = len(items)
	for _, item := range items {
		summary.TotalResponseBytes += item.ResponseBytes
		summary.TotalTokens += item.TokensStreamed
		summary.AvgDurationMs += float64(item.DurationMs)
		summary.AvgTokensPerSec += item.TokensPerSecond
	}
	if summary.RequestCount > 0 {
		summary.AvgDurationMs /= float64(summary.RequestCount)
		summary.AvgTokensPerSec /= float64(summary.RequestCount)
	}

	return Snapshot{
		Summary:  summary,
		Requests: items,
	}
}
