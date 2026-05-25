package storage

import (
	"encoding/hex"
	"log"

	"github.com/wk-y/rama-swap/server/gcas"
)

func storageLog(format string, args ...any) {
	log.Printf("[storage] "+format, args...)
}

func hashHex(h gcas.Hash) string {
	return hex.EncodeToString(h[:])
}

func logChunkManifest(path string, chunks []chunkRef) {
	storageLog("chunk manifest path=%s count=%d", path, len(chunks))
	for i, c := range chunks {
		storageLog("  chunk[%d] hash=%s size=%d", i, hashHex(c.hash), c.size)
	}
}
