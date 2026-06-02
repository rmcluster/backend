package storage

import (
	"fmt"

	"github.com/rmcluster/backend/server/scheduling"
)

const TunableChunkSizeMiB = "chunk_size_mib"

var tunableSpecs = []scheduling.TunableSpec{
	{
		Key:         TunableChunkSizeMiB,
		Label:       "Chunk size",
		Description: "Maximum file chunk size for new writes.",
		Kind:        scheduling.TunableKindInt,
		Unit:        "MiB",
		Min:         scheduling.NumberPtr(1),
		Max:         scheduling.NumberPtr(1024),
	},
}

func (s *StorageServiceImpl) TunableSpecs() []scheduling.TunableSpec {
	return append([]scheduling.TunableSpec(nil), tunableSpecs...)
}

func (s *StorageServiceImpl) TunableValues() map[string]any {
	return map[string]any{
		TunableChunkSizeMiB: s.GetChunkSize() / (1024 * 1024),
	}
}

func (s *StorageServiceImpl) ApplyTunables(values map[string]any) error {
	if len(values) == 0 {
		return fmt.Errorf("values must not be empty")
	}
	for key := range values {
		if key != TunableChunkSizeMiB {
			return fmt.Errorf("unknown tunable %q", key)
		}
	}
	chunkSizeMiB, err := scheduling.ParseTunableInt(values, TunableChunkSizeMiB)
	if err != nil {
		return err
	}
	if err := scheduling.ValidateTunableInt(tunableSpecs[0], chunkSizeMiB); err != nil {
		return err
	}
	return s.SetChunkSize(int64(chunkSizeMiB) * 1024 * 1024)
}
