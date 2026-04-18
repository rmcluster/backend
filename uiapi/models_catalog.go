package uiapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

type hfSearchResult struct {
	ID        string   `json:"id"`
	Tags      []string `json:"tags"`
	Downloads int      `json:"downloads"`
}

var hfSearchClient = http.Client{Timeout: 5 * time.Second}

func mergeModelRefs(base []string, extra []string) []string {
	out := make([]string, 0, len(base)+len(extra))
	seen := map[string]struct{}{}

	for _, value := range append(base, extra...) {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}

	return out
}

func searchHFModels(query string, limit int) ([]hfSearchResult, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 12
	}

	endpoint := "https://huggingface.co/api/models?search=" + url.QueryEscape(q) + "&limit=" + strconv.Itoa(limit)
	resp, err := hfSearchClient.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("search failed with status %d", resp.StatusCode)
	}

	results := make([]hfSearchResult, 0)
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}

	filtered := make([]hfSearchResult, 0, len(results))
	for _, item := range results {
		idLower := strings.ToLower(item.ID)
		hasGGUFTag := false
		for _, tag := range item.Tags {
			if strings.EqualFold(tag, "gguf") {
				hasGGUFTag = true
				break
			}
		}
		if hasGGUFTag || strings.Contains(idLower, "gguf") {
			filtered = append(filtered, item)
		}
	}

	if len(filtered) == 0 {
		return results, nil
	}

	return slices.Clip(filtered), nil
}
