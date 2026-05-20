package uiapi

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/wk-y/rama-swap/llama"
)

type modelEntry struct {
	DisplayName  string
	Model        string
	Parameters   string
	Architecture string
	Quantization string
	LinkHref     string
	LinkLabel    string
	Source       string
}

type customModelEntry struct {
	Model        string `json:"model"`
	DisplayName  string `json:"display_name"`
	Parameters   string `json:"parameters"`
	Architecture string `json:"architecture"`
	Quantization string `json:"quantization"`
	LinkHref     string `json:"link_href"`
	LinkLabel    string `json:"link_label"`
	Source       string `json:"source"`
}

var (
	paramsHintRe  = regexp.MustCompile(`(?i)(\d+(?:\.\d+)?\s*[mb])`)
	quantHintRe   = regexp.MustCompile(`(?i)(q\d(?:_[a-z0-9]+)*|awq|gptq|fp16|fp8|bf16|int4|int8)`)
	ggufExtRe     = regexp.MustCompile(`(?i)\.gguf$`)
	quantSuffixRe = regexp.MustCompile(`(?i)(?:[-_]?q\d(?:_[a-z0-9]+)*)$`)
)

func builtinModelEntries(models []llama.Model) []modelEntry {
	entries := make([]modelEntry, 0, len(models))
	for _, model := range models {
		ref := strings.TrimSpace(model.Name)
		if ref == "" {
			continue
		}
		displayName := simplifyModelDisplayName(ref)
		linkHref, linkLabel := modelLinkForRef(ref)
		entries = append(entries, modelEntry{
			DisplayName: displayName,
			Model:       ref,
			LinkHref:    linkHref,
			LinkLabel:   linkLabel,
			Source:      modelSourceLabel(ref),
		})
	}
	return entries
}

func customModelEntries(entries []customModelEntry) []modelEntry {
	out := make([]modelEntry, 0, len(entries))
	for _, entry := range entries {
		modelRef := strings.TrimSpace(entry.Model)
		if modelRef == "" {
			continue
		}

		displayName := strings.TrimSpace(entry.DisplayName)
		if displayName == "" {
			displayName = simplifyModelDisplayName(modelRef)
		}

		linkHref := strings.TrimSpace(entry.LinkHref)
		linkLabel := strings.TrimSpace(entry.LinkLabel)
		if linkHref == "" || linkLabel == "" {
			linkHref, linkLabel = modelLinkForRef(modelRef)
		}

		out = append(out, modelEntry{
			DisplayName:  displayName,
			Model:        modelRef,
			Parameters:   strings.TrimSpace(entry.Parameters),
			Architecture: strings.TrimSpace(entry.Architecture),
			Quantization: strings.TrimSpace(entry.Quantization),
			LinkHref:     linkHref,
			LinkLabel:    linkLabel,
			Source:       strings.TrimSpace(entry.Source),
		})
	}
	return out
}

func simplifyModelDisplayName(ref string) string {
	if strings.HasPrefix(ref, "hf:") {
		repo, variant, ok := parseHFModelRef(ref)
		if !ok {
			return ref
		}

		base := repo
		if idx := strings.LastIndex(base, "/"); idx >= 0 {
			base = base[idx+1:]
		}
		base = strings.TrimSuffix(base, "-GGUF")
		base = strings.TrimSuffix(base, "-gguf")

		if variant == "" {
			return base
		}

		candidate := ggufExtRe.ReplaceAllString(variant, "")
		candidate = quantSuffixRe.ReplaceAllString(candidate, "")
		candidate = strings.Trim(candidate, "-_")

		if candidate == "" || strings.EqualFold(candidate, "ud") || len(candidate) < 4 {
			return base
		}

		return candidate
	}

	clean := strings.TrimSpace(ref)
	clean = filepath.Base(clean)
	clean = strings.TrimSuffix(clean, ".gguf")
	clean = strings.TrimSuffix(clean, ".GGUF")
	return clean
}

func modelLinkForRef(ref string) (href string, label string) {
	if repo, _, ok := parseHFModelRef(ref); ok {
		return "https://huggingface.co/" + repo, "Repo"
	}

	abs, err := filepath.Abs(ref)
	if err != nil {
		abs = ref
	}
	return "file://" + filepath.ToSlash(abs), "File"
}

func modelSourceLabel(ref string) string {
	if strings.HasPrefix(ref, "hf:") {
		return "Hugging Face"
	}
	return "Local"
}

func inferModelParameters(nameOrPath string) string {
	match := paramsHintRe.FindString(nameOrPath)
	if match == "" {
		return ""
	}
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(match), " ", ""))
}

func inferModelQuantization(nameOrPath string) string {
	match := quantHintRe.FindString(nameOrPath)
	if match == "" {
		return ""
	}
	return strings.ToUpper(match)
}

func inferModelNameFromPath(path string) string {
	name := filepath.Base(strings.TrimSpace(path))
	name = strings.TrimSuffix(name, ".gguf")
	name = strings.TrimSuffix(name, ".GGUF")
	return name
}

func normalizeModelRefInput(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(value, "hf:") {
		return value
	}
	if strings.Contains(value, "/") {
		return "hf:" + value
	}
	return value
}

func mergeModelEntries(base []modelEntry, extra []modelEntry) []modelEntry {
	out := make([]modelEntry, 0, len(base)+len(extra))
	seen := map[string]struct{}{}
	for _, entry := range append(base, extra...) {
		key := strings.TrimSpace(entry.Model)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, entry)
	}
	return out
}

func parseLocalModelInput(name, modelPath, parameters, quantization string) (customModelEntry, error) {
	name = strings.TrimSpace(name)
	modelPath = strings.TrimSpace(modelPath)
	parameters = strings.TrimSpace(parameters)
	quantization = strings.TrimSpace(quantization)

	if modelPath == "" {
		return customModelEntry{}, fmt.Errorf("model path is required")
	}
	if !strings.EqualFold(filepath.Ext(modelPath), ".gguf") {
		return customModelEntry{}, fmt.Errorf("only .gguf models are allowed")
	}

	absPath, err := filepath.Abs(modelPath)
	if err != nil {
		return customModelEntry{}, err
	}

	if name == "" {
		name = inferModelNameFromPath(absPath)
	}
	if parameters == "" {
		parameters = inferModelParameters(name)
		if parameters == "" {
			parameters = inferModelParameters(absPath)
		}
	}
	if quantization == "" {
		quantization = inferModelQuantization(name)
		if quantization == "" {
			quantization = inferModelQuantization(absPath)
		}
	}

	if name == "" {
		return customModelEntry{}, fmt.Errorf("name is required")
	}
	if parameters == "" {
		return customModelEntry{}, fmt.Errorf("parameters are required if they cannot be inferred")
	}
	if quantization == "" {
		return customModelEntry{}, fmt.Errorf("quantization is required if it cannot be inferred")
	}

	return customModelEntry{
		Model:        absPath,
		DisplayName:  name,
		Parameters:   parameters,
		Quantization: quantization,
		LinkHref:     "file://" + filepath.ToSlash(absPath),
		LinkLabel:    "File",
		Source:       "Local",
	}, nil
}

func parseHFModelAddInput(value string) (customModelEntry, error) {
	value = normalizeModelRefInput(value)
	if value == "" {
		return customModelEntry{}, fmt.Errorf("model repo is required")
	}
	if !strings.HasPrefix(value, "hf:") {
		return customModelEntry{}, fmt.Errorf("use owner/repo or hf:owner/repo format")
	}
	if repo, _, ok := parseHFModelRef(value); !ok || repo == "" {
		return customModelEntry{}, fmt.Errorf("invalid Hugging Face model reference")
	}

	return customModelEntry{
		Model:       value,
		DisplayName: simplifyModelDisplayName(value),
		LinkHref:    func() string { repo, _, _ := parseHFModelRef(value); return "https://huggingface.co/" + repo }(),
		LinkLabel:   "Repo",
		Source:      "Hugging Face",
	}, nil
}
