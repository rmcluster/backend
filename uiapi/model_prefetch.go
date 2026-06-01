package uiapi

import (
	"context"
	"log"

	"github.com/rmcluster/backend/llama"
)

func (s *UIApi) listModelCache() []apiCacheEntry {
	cachedModels, err := s.llama.GetCachedModels()
	if err != nil {
		log.Printf("failed to read llama cache list: %v", err)
		return nil
	}

	out := make([]apiCacheEntry, 0, len(cachedModels))
	for _, item := range cachedModels {
		out = append(out, apiCacheEntry{Repo: item.Repo, Quant: item.Quant})
	}
	return out
}

func (s *UIApi) startPrefetch(modelRef string) {
	go s.prefetchModel(modelRef)
}

func (s *UIApi) prefetchModel(modelRef string) {
	repo, _, ok := parseHFModelRef(modelRef)
	if !ok || repo == "" {
		return
	}

	cmd := s.llama.ServeCommand(context.Background(), llama.ServeArgs{
		Model: modelRef,
		Port: 0,
	})

	if err := cmd.Start(); err != nil {
		log.Printf("prefetch start failed for %s: %v", modelRef, err)
		return
	}

	if err := cmd.Wait(); err != nil {
		log.Printf("prefetch finished for %s: %v", modelRef, err)
	}
}
