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

	endpoint := "https://huggingface.co/api/models?search=" + url.QueryEscape(q) + "&filter=gguf&limit=" + strconv.Itoa(limit)
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

func validateHFRepoHasGGUF(modelRef string) error {
	repo, _, ok := parseHFModelRef(modelRef)
	if !ok || repo == "" {
		return fmt.Errorf("invalid Hugging Face model reference")
	}

	resp, err := hfSearchClient.Get("https://huggingface.co/api/models/" + repo)
	if err != nil {
		return fmt.Errorf("failed to look up Hugging Face repository")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("repository not found on Hugging Face")
	}

	var item hfSearchResult
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return fmt.Errorf("failed to read Hugging Face repository metadata")
	}

	idLower := strings.ToLower(item.ID)
	for _, tag := range item.Tags {
		if strings.EqualFold(tag, "gguf") {
			return nil
		}
	}
	if strings.Contains(idLower, "gguf") {
		return nil
	}

	return fmt.Errorf("repository has no GGUF files; search for a GGUF repo or pick a specific .gguf file")
}
