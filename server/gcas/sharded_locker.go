package gcas

import "sync"

// number of bytes in the hash to use for sharding
const SHARD_BYTES = 2

// shardedLocker locks hashes by using multiple sync.RWMutexes.
// it provides better concurrency than a single sync.RWMutex, without the complexity of per-hash mutexes.
type shardedLocker struct {
	mutexes [1 << (SHARD_BYTES * 8)]sync.RWMutex
}

// newShardedLocker creates a new shardedLocker.
func newShardedLocker() *shardedLocker {
	return &shardedLocker{}
}

func (s *shardedLocker) getShardIdx(hash Hash) uint16 {
	return uint16(hash[0]) | uint16(hash[1])<<8
}

func (s *shardedLocker) RLock(hash Hash) {
	idx := s.getShardIdx(hash)
	s.mutexes[idx].RLock()
}

func (s *shardedLocker) RUnlock(hash Hash) {
	idx := s.getShardIdx(hash)
	s.mutexes[idx].RUnlock()
}

func (s *shardedLocker) Lock(hash Hash) {
	idx := s.getShardIdx(hash)
	s.mutexes[idx].Lock()
}

func (s *shardedLocker) Unlock(hash Hash) {
	idx := s.getShardIdx(hash)
	s.mutexes[idx].Unlock()
}
