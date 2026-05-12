package gcas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
)

// NewGCAS creates a new GCAS instance.
// db is the database connection to use for storing metadata
func NewGCAS(db *sql.DB) GCAS {
	return &GcasImpl{
		db:            db,
		nodes:         make(map[string]CAS),
		shardedLocker: newShardedLocker(),
	}
}

type GcasImpl struct {
	db *sql.DB
	// nodes connected to the cluster
	nodesLock       sync.RWMutex
	nodes           map[string]CAS
	shardedLocker   *shardedLocker
	maintenanceLock sync.Mutex // enforces that at most one maintenance runs at a time
}

// ReplaceNode implements [GCAS].
func (g *GcasImpl) ReplaceNode(node NamedCAS) {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()
	g.nodes[node.Name()] = node
}

// AddNode implements [GCAS].
func (g *GcasImpl) AddNode(node NamedCAS) {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()
	g.nodes[node.Name()] = node
}

// RemoveNode implements [GCAS].
func (g *GcasImpl) RemoveNode(nodeName string) {
	g.nodesLock.Lock()
	defer g.nodesLock.Unlock()
	delete(g.nodes, nodeName)
}

// Delete implements [CAS].
func (g *GcasImpl) Delete(ctx context.Context, hash Hash) error {
	g.shardedLocker.Lock(hash)
	defer g.shardedLocker.Unlock(hash)

	// soft delete from the database
	// must check is_data = 1 to get the correct number of rows affected for response code
	result, err := g.db.ExecContext(ctx, "UPDATE chunks SET is_data = 0 WHERE hash = ? AND is_data = 1", hash[:])
	if err != nil {
		return err
	}

	// if no rows were updated, the chunk does not exist
	numRowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if numRowsAffected == 0 {
		return HashNotFoundError{}
	}

	return nil
}

// FreeSpace implements [CAS].
func (g *GcasImpl) FreeSpace(ctx context.Context) (int64, error) {
	// sum up free space of all connected nodes
	var sum int64
	errs := []error{}
	type sumResult struct {
		free int64
		err  error
	}
	resultChan := make(chan sumResult)

	g.nodesLock.RLock()
	count := len(g.nodes)
	for _, node := range g.nodes {
		// note: since Go 1.22 for loops bind per iteration
		go func() {
			free, err := node.FreeSpace(ctx)
			resultChan <- sumResult{
				free: free,
				err:  err,
			}
		}()
	}
	g.nodesLock.RUnlock()

	for i := 0; i < count; i++ {
		res := <-resultChan
		if res.err != nil {
			errs = append(errs, res.err)
		} else {
			sum += res.free
		}
	}

	if len(errs) > 0 {
		return sum, errors.Join(errs...)
	}

	return sum, nil
}

// Get implements [CAS].
func (g *GcasImpl) Get(ctx context.Context, hash Hash) ([]byte, error) {
	g.shardedLocker.RLock(hash)
	defer g.shardedLocker.RUnlock(hash)

	var nodeID string
	err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ? AND is_data = 1", hash[:]).Scan(&nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, HashNotFoundError{}
		}
		return nil, err
	}

	g.nodesLock.RLock()
	cas, ok := g.nodes[nodeID]
	g.nodesLock.RUnlock()
	if ok {
		return cas.Get(ctx, hash)
	}

	// if the chunk exists but the node is not connected, give a server error
	return nil, errors.New("node not connected")
}

// List implements [CAS].
func (g *GcasImpl) List(ctx context.Context) (<-chan Hash, error) {
	visited := make(map[Hash]struct{})
	ch := make(chan Hash)
	// the list of nodes might change while we are iterating over it.
	// holding the lock while iterating could result in a deadlock if the channel is not drained.
	// thus we copy the list of nodes first, accepting that the list might not be up to date.
	g.nodesLock.RLock()
	nodes := make([]CAS, 0, len(g.nodes))
	for _, node := range g.nodes {
		nodes = append(nodes, node)
	}
	g.nodesLock.RUnlock()

	go func() {
		defer close(ch)
		for _, node := range nodes {
			hashes, err := node.List(ctx)
			if err != nil {
				return
			}
			for hash := range hashes {
				if _, ok := visited[hash]; ok {
					continue
				}
				visited[hash] = struct{}{}
				select {
				case ch <- hash:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch, nil
}

// Put implements [CAS].
func (g *GcasImpl) Put(ctx context.Context, hash Hash, data []byte) error {
	g.shardedLocker.Lock(hash)
	defer g.shardedLocker.Unlock(hash)

	// pick a random node to store the chunk
	// note: golang internally randomizes the starting point of map iteration,
	// however this is not guaranteed and not meant to be relied upon.

	// check if the chunk already exists
	{
		var nodeID string
		err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ? AND is_data = 1", hash[:]).Scan(&nodeID)
		if err != sql.ErrNoRows {
			if err != nil {
				return err
			}

			// if the chunk already exists, return HashExistsError
			return HashExistsError{}
		}

		// try to update is_data to 1 from 0 (from deleted) if the chunk exists
		// the AND clause is needed to differentiate between a deleted chunk and a non-existent chunk
		result, err := g.db.ExecContext(ctx, "UPDATE chunks SET is_data = 1 WHERE hash = ? AND is_data = 0", hash[:])
		if err != nil {
			return err
		}

		numRowsAffected, _ := result.RowsAffected()
		if numRowsAffected != 0 {
			// the deleted chunk has been re-added
			return nil
		}
	}

	type nodePair struct {
		id  string
		cas CAS
	}

	g.nodesLock.RLock()
	nodes := make([]nodePair, 0, len(g.nodes))
	for id := range g.nodes {
		nodes = append(nodes, nodePair{
			id:  id,
			cas: g.nodes[id],
		})
	}
	g.nodesLock.RUnlock()

	if len(nodes) == 0 {
		return ErrNoNodes{}
	}

	idx := rand.Intn(len(nodes))
	node := nodes[idx]

	err := node.cas.Put(ctx, hash, data)

	if err != nil {
		return err
	}

	_, err = g.db.ExecContext(ctx, "INSERT INTO chunks (hash, size, node_id) VALUES (?, ?, ?)", hash[:], len(data), node.id)
	return err
}

// RunGC runs the garbage collection process.
// It deletes chunks that have been marked as deleted
func (g *GcasImpl) RunGC(ctx context.Context) error {
	// remove all chunks that have been marked as deleted and are not used for parity
	rows, err := g.db.QueryContext(ctx, "DELETE FROM chunks WHERE is_data = 0 AND NOT EXISTS (SELECT 1 FROM erasure_group_member WHERE hash_id = chunks.hash) RETURNING hash, node_id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		// delete chunks from the node
		// ignore failures to delete; this will be handled by a deeper garbage collection
		var hash Hash
		var nodeID string
		if err := rows.Scan(hash[:], &nodeID); err != nil {
			continue
		}

		g.nodesLock.RLock()
		cas, ok := g.nodes[nodeID]
		g.nodesLock.RUnlock()
		if !ok {
			continue
		}
		if err := cas.Delete(ctx, hash); err != nil {
			continue
		}
	}
	return nil
}

// RunMaintenance does a one-off maintenance cycle.
func (g *GcasImpl) RunMaintenance(ctx context.Context) error {
	lock := g.maintenanceLock.TryLock()
	if !lock {
		return fmt.Errorf("maintenance already running")
	}
	defer g.maintenanceLock.Unlock()

	if err := g.RunGC(ctx); err != nil {
		log.Printf("error while running gc: %v", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}

var _ GCAS = (*GcasImpl)(nil)
