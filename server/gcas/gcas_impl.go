package gcas

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
)

// NewGCAS creates a new GCAS instance.
// db is the database connection to use for storing metadata
func NewGCAS(db *sql.DB) GCAS {
	gcas := NewGcasImpl(db)
	return gcas
}

type GcasImpl struct {
	db *sql.DB
	// nodes connected to the cluster
	nodesLock sync.RWMutex
	nodes     map[string]CAS
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

func NewGcasImpl(db *sql.DB) *GcasImpl {
	return &GcasImpl{
		db:    db,
		nodes: make(map[string]CAS),
	}
}

// Delete implements [CAS].
func (g *GcasImpl) Delete(ctx context.Context, hash Hash) error {
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
	if cas, ok := g.nodes[nodeID]; ok {
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
	for nodeID, node := range g.nodes {
		free, err := node.FreeSpace(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to get free space from node %s: %w", nodeID, err)
		}
		sum += free
	}
	return sum, nil
}

// Get implements [CAS].
func (g *GcasImpl) Get(ctx context.Context, hash Hash) ([]byte, error) {
	var nodeID string
	err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ?", hash[:]).Scan(&nodeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, HashNotFoundError{}
		}
		return nil, err
	}

	if cas, ok := g.nodes[nodeID]; ok {
		return cas.Get(ctx, hash)
	}

	// if the chunk exists but the node is not connected, give a server error
	return nil, errors.New("node not connected")
}

// List implements [CAS].
func (g *GcasImpl) List(ctx context.Context) (<-chan Hash, error) {
	visited := make(map[Hash]struct{})
	ch := make(chan Hash)
	go func() {
		defer close(ch)
		for _, node := range g.nodes {
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
	// pick a random node to store the chunk
	// note: golang internally randomizes the starting point of map iteration,
	// however this is not guaranteed and not meant to be relied upon.

	var nodes []string

	g.nodesLock.RLock()
	defer g.nodesLock.RUnlock()
	for id := range g.nodes {
		nodes = append(nodes, id)
	}

	if len(nodes) == 0 {
		return errors.New("no nodes available")
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
