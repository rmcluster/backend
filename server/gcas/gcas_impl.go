package gcas

import (
	"context"
	"database/sql"
	"errors"
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
	nodesLock     sync.RWMutex
	nodes         map[string]CAS
	shardedLocker *shardedLocker
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

	// which node has the chunk?
	// query chunks table in database
	var nodeID string
	err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ?", hash[:]).Scan(&nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return HashNotFoundError{}
		}
		return err
	}

	// if the node is currently connected, call Delete on the node's CAS
	g.nodesLock.RLock()
	cas, ok := g.nodes[nodeID]
	g.nodesLock.RUnlock()

	if ok {
		err = cas.Delete(ctx, hash)
		// if delete failed for any reason other than HashNotFoundError, propagate without touching the database
		if err != nil && !errors.Is(err, HashNotFoundError{}) {
			return err
		}
	}

	// delete from the database
	_, err = g.db.ExecContext(ctx, "DELETE FROM chunks WHERE hash = ?", hash[:])
	if err != nil {
		return err
	}
	return nil
}

// FreeSpace implements [CAS].
func (g *GcasImpl) FreeSpace(ctx context.Context) (int64, error) {
	// sum up free space of all connected nodes
	var sum int64
	errs := []error{}
	var count int
	type sumResult struct {
		free int64
		err  error
	}
	resultChan := make(chan sumResult)

	go func() {
		g.nodesLock.RLock()
		defer g.nodesLock.RUnlock()
		count = len(g.nodes)
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
	}()

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
	err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ?", hash[:]).Scan(&nodeID)
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

	var nodes []string

	// check if the chunk already exists
	{
		var nodeID string
		err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ?", hash[:]).Scan(&nodeID)
		if err != sql.ErrNoRows {
			if err != nil {
				return err
			}

			// if the chunk already exists, return HashExistsError
			return HashExistsError{}
		}
	}

	g.nodesLock.RLock()
	defer g.nodesLock.RUnlock()
	for id := range g.nodes {
		nodes = append(nodes, id)
	}

	if len(nodes) == 0 {
		return ErrNoNodes{}
	}

	idx := rand.Intn(len(nodes))
	nodeID := nodes[idx]

	cas := g.nodes[nodeID]
	err := cas.Put(ctx, hash, data)

	if err != nil {
		return err
	}

	_, err = g.db.ExecContext(ctx, "INSERT INTO chunks (hash, size, node_id) VALUES (?, ?, ?)", hash[:], len(data), nodeID)
	return err
}

var _ GCAS = (*GcasImpl)(nil)
