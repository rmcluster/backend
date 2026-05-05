package uiapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

type hfMetadata struct {
	Parameters       string
	Architecture     string
	Quantization     string
	SupportsThinking bool
}

type hfModelResponse struct {
	Tags      []string       `json:"tags"`
	Config    map[string]any `json:"config"`
	CardData  map[string]any `json:"cardData"`
	BaseModel any            `json:"base_model"`
	GGUF      map[string]any `json:"gguf"`
}

var hfClient = http.Client{Timeout: 4 * time.Second}

var (
	hfCacheMu sync.RWMutex
	hfCache   = map[string]hfMetadata{}
)

var (
	paramsTagRe = regexp.MustCompile(`(?i)\b\d+(?:\.\d+)?b\b`)
	quantTagRe  = regexp.MustCompile(`(?i)\b(?:q\d+(?:_[a-z0-9]+)*|\d+-bit|awq|gptq|fp16|fp8|bf16)\b`)
)

func parseHFModelRef(name string) (repo string, variant string, ok bool) {
	if !strings.HasPrefix(name, "hf:") {
		return "", "", false
	}

	trimmed := strings.TrimPrefix(name, "hf:")
	parts := strings.SplitN(trimmed, ":", 2)
	repo = strings.TrimSpace(parts[0])
	if repo == "" {
		return "", "", false
	}
	if len(parts) == 2 {
		variant = strings.TrimSpace(parts[1])
	}
	return repo, variant, true
}

// hfMetadataCacheVersion is bumped whenever the hfMetadata struct gains new
// fields, so stale BoltDB entries are ignored rather than returning zero values.
const hfMetadataCacheVersion = "v2:"

func fetchHFMetadata(repo string, variant string) hfMetadata {
	key := hfMetadataCacheVersion + repo + "::" + variant

	hfCacheMu.RLock()
	if cached, ok := hfCache[key]; ok {
		hfCacheMu.RUnlock()
		return cached
	}
	hfCacheMu.RUnlock()

	if hfStore != nil {
		if cached, ok, err := hfStore.Get(key); err == nil && ok {
			cacheHFMetadata(repo, variant, cached)
			return cached
		}
	}

	meta := hfMetadata{Parameters: "-", Architecture: "-", Quantization: "-"}

	endpoint := "https://huggingface.co/api/models/" + repo
	resp, err := hfClient.Get(endpoint)
	if err != nil {
		return meta
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return meta
	}

	var payload hfModelResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return meta
	}

	meta.Parameters = firstNonEmpty(
		readString(payload.CardData, "params"),
		readString(payload.CardData, "parameter_count"),
		readString(payload.CardData, "parameters"),
		extractParamsFromBaseModel(readString(payload.CardData, "base_model")),
		extractParamsFromBaseModel(readBaseModelValue(payload.BaseModel)),
		extractParamsFromName(repo),
		readParamsFromTags(payload.Tags),
	)

	meta.Architecture = firstNonEmpty(
		readString(payload.GGUF, "architecture"),
		readString(payload.CardData, "architecture"),
		readFirstStringSlice(payload.CardData, "architectures"),
		readFirstStringSlice(payload.Config, "architectures"),
		extractArchitectureFromTags(payload.Tags),
	)

	meta.Quantization = firstNonEmpty(
		readString(payload.CardData, "quantization"),
		readQuantFromTags(payload.Tags),
		extractQuantFromVariant(variant),
	)

	meta.SupportsThinking = chatTemplateContainsThink(payload.Config)
	// GGUF repos rarely include tokenizer_config. If we didn't find a <think>
	// template here, try the upstream base model repo.
	if !meta.SupportsThinking {
		if baseRepo := extractBaseModelRepo(payload.Tags); baseRepo != "" && baseRepo != repo {
			if baseMeta := fetchHFMetadata(baseRepo, ""); baseMeta.SupportsThinking {
				meta.SupportsThinking = true
			}
		}
	}

	if meta.Parameters == "" {
		meta.Parameters = "-"
	}
	if meta.Architecture == "" {
		meta.Architecture = "-"
	}
	if meta.Quantization == "" {
		meta.Quantization = "-"
	}

	cacheHFMetadata(repo, variant, meta)
	if hfStore != nil {
		_ = hfStore.Set(key, meta)
	}
	return meta
}

func cacheHFMetadata(repo string, variant string, meta hfMetadata) {
	hfCacheMu.Lock()
	hfCache[hfMetadataCacheVersion+repo+"::"+variant] = meta
	hfCacheMu.Unlock()
}

func readString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(v))
}

func readFirstStringSlice(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}

	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(arr[0]))
}

func readParamsFromTags(tags []string) string {
	for _, tag := range tags {
		if strings.HasPrefix(tag, "base_model:") {
			if params := extractParamsFromBaseModel(tag); params != "" {
				return params
			}
		}
		if match := paramsTagRe.FindString(tag); match != "" {
			return strings.ToUpper(match)
		}
	}
	return ""
}

func readBaseModelValue(value any) string {
	switch v := value.(type) {
	case string:
		return strings.TrimSpace(v)
	case []any:
		if len(v) == 0 {
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(v[0]))
	default:
		return ""
	}
}

func extractParamsFromBaseModel(value string) string {
	if value == "" {
		return ""
	}
	if match := paramsTagRe.FindString(value); match != "" {
		return strings.ToUpper(match)
	}
	return ""
}

func extractParamsFromName(repo string) string {
	if repo == "" {
		return ""
	}
	if match := paramsTagRe.FindString(repo); match != "" {
		return strings.ToUpper(match)
	}
	return ""
}

func extractArchitectureFromTags(tags []string) string {
	for _, tag := range tags {
		if strings.HasPrefix(tag, "base_model:") {
			value := strings.TrimPrefix(tag, "base_model:")
			if strings.HasPrefix(value, "quantized:") {
				value = strings.TrimPrefix(value, "quantized:")
			}
			if value != "" {
				parts := strings.Split(value, "/")
				if len(parts) > 0 {
					name := parts[len(parts)-1]
					if name != "" {
						return name
					}
				}
			}
		}
	}
	return ""
}

func readQuantFromTags(tags []string) string {
	for _, tag := range tags {
		if match := quantTagRe.FindString(tag); match != "" {
			return strings.ToUpper(match)
		}
	}
	return ""
}

func extractQuantFromVariant(variant string) string {
	if variant == "" {
		return ""
	}
	if match := quantTagRe.FindString(variant); match != "" {
		return strings.ToUpper(match)
	}
	return ""
}

// extractBaseModelRepo returns the first "owner/repo" found in base_model tags,
// stripping the "quantized:" qualifier if present. Returns "" if none found.
func extractBaseModelRepo(tags []string) string {
	for _, tag := range tags {
		val, ok := strings.CutPrefix(tag, "base_model:")
		if !ok {
			continue
		}
		// skip "base_model:quantized:owner/repo" qualifier prefix
		val, _ = strings.CutPrefix(val, "quantized:")
		val = strings.TrimSpace(val)
		if strings.Contains(val, "/") {
			return val
		}
	}
	return ""
}

// chatTemplateContainsThink returns true when the HF model config includes a
// tokenizer chat_template that uses <think> blocks (e.g. Qwen3, DeepSeek-R1).
func chatTemplateContainsThink(config map[string]any) bool {
	if config == nil {
		return false
	}
	tc, ok := config["tokenizer_config"].(map[string]any)
	if !ok {
		return false
	}
	template, _ := tc["chat_template"].(string)
	return strings.Contains(template, "<think>")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" && value != "<nil>" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
