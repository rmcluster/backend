package llama

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
)

// CachedModel is one Hugging Face model already present in the local llama.cpp HF cache.
type CachedModel struct {
	Repo  string // e.g. unsloth/gemma-3-1b-it-GGUF
	Quant string // e.g. Q4_K_M
}

// GetCachedModels runs `llama-server --cache-list` and parses its stdout.
func (r Llama) GetCachedModels() ([]CachedModel, error) {
	if err := r.checkValidity(); err != nil {
		return nil, err
	}

	cliArgs := append(append([]string{}, r.Command[1:]...), "--cache-list")
	cmd := exec.Command(r.Command[0], cliArgs...)

	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list cached models: %w", err)
	}

	models := make([]CachedModel, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		raw, ok := cacheListEntry(scanner.Text())
		if !ok {
			continue
		}
		repo, quant, ok := parseCachedModelRef(raw)
		if !ok {
			continue
		}
		models = append(models, CachedModel{Repo: repo, Quant: quant})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to parse cached models: %w", err)
	}

	return models, nil
}

// cacheListEntry extracts "owner/repo:Q4_K_M" from a line like "   1. owner/repo:Q4_K_M".
func cacheListEntry(line string) (string, bool) {
	line = strings.TrimSpace(line)
	if i := strings.Index(line, ". "); i >= 0 {
		entry := strings.TrimSpace(line[i+2:])
		return entry, entry != ""
	}
	return "", false
}

func parseCachedModelRef(value string) (repo string, quant string, ok bool) {
	repo, quant, found := strings.Cut(value, ":")
	repo = strings.TrimSpace(repo)
	quant = strings.ToUpper(strings.TrimSpace(quant))
	if !found || repo == "" || quant == "" {
		return "", "", false
	}
	return repo, quant, true
}
