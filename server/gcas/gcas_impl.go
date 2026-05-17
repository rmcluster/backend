package gcas

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const defaultDataShards = 4
const parityShards = 2

// NewGCAS creates a new GCAS instance with the default number of data shards.
func NewGCAS(db *sql.DB) GCAS {
	return NewGCASWithDataShards(db, defaultDataShards)
}

// NewGCASWithDataShards creates a new GCAS instance with the given number of data shards per stripe.
// A stripe requires dataShards+2 distinct nodes to form. Fewer nodes disables stripe formation.
func NewGCASWithDataShards(db *sql.DB, dataShards int) GCAS {
	return &GcasImpl{
		db:            db,
		dataShards:    dataShards,
		nodes:         make(map[string]CAS),
		shardedLocker: newShardedLocker(),
	}
}

type GcasImpl struct {
	db         *sql.DB
	dataShards int
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

	var primaryErr error
	if ok {
		data, err := cas.Get(ctx, hash)
		if err == nil {
			return data, nil
		}
		primaryErr = err
	} else {
		primaryErr = errors.New("node not connected")
	}

	// attempt erasure coding recovery
	data, err := g.ecRecover(ctx, hash)
	if err == nil {
		return data, nil
	}

	return nil, primaryErr
}

// ecRecover attempts to reconstruct a data chunk from its erasure group.
// Returns an error if the chunk has no erasure group or recovery is not possible.
func (g *GcasImpl) ecRecover(ctx context.Context, hash Hash) ([]byte, error) {
	type memberRow struct {
		sliceIdx int
		hashID   Hash
		size     int
		nodeID   string
	}

	// look up the erasure group for this chunk
	var groupID int64
	var dataShards, pShards, shardSize int
	err := g.db.QueryRowContext(ctx, `
		SELECT eg.id, eg.data_shards, eg.parity_shards, eg.shard_size
		FROM erasure_group eg
		JOIN erasure_group_member egm ON egm.erasure_group_id = eg.id
		WHERE egm.hash_id = ?`, hash[:]).Scan(&groupID, &dataShards, &pShards, &shardSize)
	if err != nil {
		return nil, fmt.Errorf("not in any erasure group: %w", err)
	}

	// load all members
	rows, err := g.db.QueryContext(ctx, `
		SELECT egm.slice_idx, egm.hash_id, c.size, c.node_id
		FROM erasure_group_member egm
		JOIN chunks c ON c.hash = egm.hash_id
		WHERE egm.erasure_group_id = ?`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	members := make([]memberRow, 0, dataShards+pShards)
	for rows.Next() {
		var m memberRow
		var hashBytes []byte
		if err := rows.Scan(&m.sliceIdx, &hashBytes, &m.size, &m.nodeID); err != nil {
			return nil, err
		}
		if len(hashBytes) != len(Hash{}) {
			continue
		}
		copy(m.hashID[:], hashBytes)
		members = append(members, m)
	}

	// build the target slice index
	targetSlice := -1
	for _, m := range members {
		if m.hashID == hash {
			targetSlice = m.sliceIdx
			break
		}
	}
	if targetSlice < 0 || targetSlice >= dataShards {
		return nil, errors.New("target chunk is not a data shard")
	}

	// fetch each shard; nil means unavailable
	shards := make([][]byte, dataShards+pShards)
	g.nodesLock.RLock()
	nodesCopy := make(map[string]CAS, len(g.nodes))
	for k, v := range g.nodes {
		nodesCopy[k] = v
	}
	g.nodesLock.RUnlock()

	present := 0
	for _, m := range members {
		cas, ok := nodesCopy[m.nodeID]
		if !ok {
			continue
		}
		data, err := cas.Get(ctx, m.hashID)
		if err != nil {
			continue
		}
		// pad to shard_size so RS can operate on equal-length slices
		padded := make([]byte, shardSize)
		copy(padded, data)
		shards[m.sliceIdx] = padded
		present++
	}

	if present < dataShards {
		return nil, fmt.Errorf("only %d/%d shards available for recovery", present, dataShards)
	}

	enc, err := reedsolomon.New(dataShards, pShards)
	if err != nil {
		return nil, err
	}
	if err := enc.ReconstructData(shards); err != nil {
		return nil, err
	}

	// find original size of the target chunk
	origSize := 0
	for _, m := range members {
		if m.hashID == hash {
			origSize = m.size
			break
		}
	}

	result := shards[targetSlice][:origSize]

	// verify hash
	if sha256.Sum256(result) != hash {
		return nil, DataCorruptError{}
	}
	return result, nil
}

// List implements [CAS].
func (g *GcasImpl) List(ctx context.Context) (<-chan Hash, error) {
	// select only data chunks (not parity)
	rows, err := g.db.QueryContext(ctx, "SELECT hash FROM chunks WHERE is_data = 1")
	if err != nil {
		return nil, err
	}

	ch := make(chan Hash)

	go func() {
		defer close(ch)
		defer rows.Close()

		for rows.Next() {
			var h []byte
			err := rows.Scan(&h)

			if len(h) != len(Hash{}) {
				log.Printf("List: Invalid hash length %d (expected %d)", len(h), len(Hash{}))
				continue
			}

			if err != nil {
				log.Printf("List: Error scanning hash: %v", err)
				return
			}

			var hash Hash
			copy(hash[:], h)

			select {
			case ch <- hash:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

// Put implements [CAS].
func (g *GcasImpl) Put(ctx context.Context, hash Hash, data []byte) error {
	g.shardedLocker.Lock(hash)
	defer g.shardedLocker.Unlock(hash)

	// check if the chunk already exists
	{
		var nodeID string
		err := g.db.QueryRowContext(ctx, "SELECT node_id FROM chunks WHERE hash = ? AND is_data = 1", hash[:]).Scan(&nodeID)
		if err != sql.ErrNoRows {
			if err != nil {
				return err
			}
			return HashExistsError{}
		}

		// try to update is_data to 1 from 0 (from deleted) if the chunk exists
		result, err := g.db.ExecContext(ctx, "UPDATE chunks SET is_data = 1 WHERE hash = ? AND is_data = 0", hash[:])
		if err != nil {
			return err
		}

		numRowsAffected, _ := result.RowsAffected()
		if numRowsAffected != 0 {
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

	if err != nil && !errors.Is(err, HashExistsError{}) {
		return err
	}

	_, err = g.db.ExecContext(ctx, "INSERT INTO chunks (hash, size, node_id) VALUES (?, ?, ?)", hash[:], len(data), node.id)
	return err
}

// RunGC runs the garbage collection process.
func (g *GcasImpl) RunGC(ctx context.Context) error {
	// clean up erasure groups whose data chunks are all deleted
	_, err := g.db.ExecContext(ctx, `
		DELETE FROM erasure_group
		WHERE id NOT IN (
			SELECT DISTINCT egm.erasure_group_id
			FROM erasure_group_member egm
			JOIN chunks ON chunks.hash = egm.hash_id
			WHERE chunks.is_data = 1
		)`)
	if err != nil {
		return err
	}

	// remove members of deleted groups (sqlite has no cascade here)
	_, err = g.db.ExecContext(ctx, `
		DELETE FROM erasure_group_member
		WHERE erasure_group_id NOT IN (SELECT id FROM erasure_group)`)
	if err != nil {
		return err
	}

	// remove all chunks that have been marked as deleted and are not used for parity
	rows, err := g.db.QueryContext(ctx, "DELETE FROM chunks WHERE is_data = 0 AND NOT EXISTS (SELECT 1 FROM erasure_group_member WHERE hash_id = chunks.hash) RETURNING hash, node_id")
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
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

type chunkInfo struct {
	hash   Hash
	size   int
	nodeID string
}

// formStripes groups unstriped data chunks into erasure-coded stripes and stores parity.
// A stripe requires g.dataShards data chunks (each on a distinct node) plus 2 parity nodes.
func (g *GcasImpl) formStripes(ctx context.Context) error {
	for {
		// fetch unstriped data chunks ordered by hash for determinism
		rows, err := g.db.QueryContext(ctx, `
			SELECT hash, size, node_id FROM chunks
			WHERE is_data = 1
			  AND hash NOT IN (SELECT hash_id FROM erasure_group_member)
			ORDER BY hash`)
		if err != nil {
			return err
		}

		// group by node_id, one chunk per node (no two data chunks on the same node in a stripe)
		seen := make(map[string]bool)
		var batch []chunkInfo
		for rows.Next() {
			var ci chunkInfo
			var hashBytes []byte
			if err := rows.Scan(&hashBytes, &ci.size, &ci.nodeID); err != nil {
				rows.Close()
				return err
			}
			if len(hashBytes) != len(Hash{}) {
				continue
			}
			if seen[ci.nodeID] {
				continue
			}
			seen[ci.nodeID] = true
			copy(ci.hash[:], hashBytes)
			batch = append(batch, ci)
			if len(batch) == g.dataShards {
				break
			}
		}
		rows.Close()

		if len(batch) < g.dataShards {
			return nil // not enough distinct-node chunks for a full stripe
		}

		if err := g.encodeStripe(ctx, batch); err != nil {
			log.Printf("formStripes: failed to encode stripe: %v", err)
			return nil // non-fatal; try again next maintenance
		}
	}
}

// encodeStripe computes parity for the given data chunks, stores parity on nodes,
// and records the erasure group in the DB.
func (g *GcasImpl) encodeStripe(ctx context.Context, dataChunks []chunkInfo) error {
	k := len(dataChunks)
	m := parityShards

	// snapshot nodes so we can pick parity destinations
	g.nodesLock.RLock()
	nodesCopy := make(map[string]CAS, len(g.nodes))
	for id, cas := range g.nodes {
		nodesCopy[id] = cas
	}
	g.nodesLock.RUnlock()

	// read data for each chunk
	shards := make([][]byte, k+m)
	shardSize := 0
	for i, ci := range dataChunks {
		cas, ok := nodesCopy[ci.nodeID]
		if !ok {
			return fmt.Errorf("node %s not connected", ci.nodeID)
		}
		data, err := cas.Get(ctx, ci.hash)
		if err != nil {
			return fmt.Errorf("read chunk %x: %w", ci.hash[:4], err)
		}
		shards[i] = data
		if len(data) > shardSize {
			shardSize = len(data)
		}
	}

	if shardSize == 0 {
		shardSize = 1 // reedsolomon requires non-zero shard size
	}

	// pad data shards to shardSize
	for i := range dataChunks {
		if len(shards[i]) < shardSize {
			padded := make([]byte, shardSize)
			copy(padded, shards[i])
			shards[i] = padded
		}
	}
	// allocate parity shards
	for i := k; i < k+m; i++ {
		shards[i] = make([]byte, shardSize)
	}

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return err
	}
	if err := enc.Encode(shards); err != nil {
		return err
	}

	// collect nodes already used by data chunks
	usedNodes := make(map[string]bool, k)
	for _, ci := range dataChunks {
		usedNodes[ci.nodeID] = true
	}

	// pick m distinct nodes not in usedNodes for parity
	parityNodes := make([]struct {
		id  string
		cas CAS
	}, 0, m)
	for id, cas := range nodesCopy {
		if !usedNodes[id] {
			parityNodes = append(parityNodes, struct {
				id  string
				cas CAS
			}{id, cas})
			usedNodes[id] = true
			if len(parityNodes) == m {
				break
			}
		}
	}
	if len(parityNodes) < m {
		return fmt.Errorf("not enough distinct nodes for parity (%d available, need %d)", len(parityNodes), m)
	}

	// store parity chunks
	type parityRecord struct {
		hash   Hash
		nodeID string
	}
	parityRecords := make([]parityRecord, m)
	for i := 0; i < m; i++ {
		ph := sha256.Sum256(shards[k+i])
		parityLock := ph
		g.shardedLocker.Lock(parityLock)

		err := parityNodes[i].cas.Put(ctx, ph, shards[k+i])
		if err != nil && !errors.Is(err, HashExistsError{}) {
			g.shardedLocker.Unlock(parityLock)
			return fmt.Errorf("store parity shard %d: %w", i, err)
		}
		_, dbErr := g.db.ExecContext(ctx,
			"INSERT OR IGNORE INTO chunks (hash, size, node_id, is_data) VALUES (?, ?, ?, 0)",
			ph[:], shardSize, parityNodes[i].id)
		g.shardedLocker.Unlock(parityLock)
		if dbErr != nil {
			return dbErr
		}
		parityRecords[i] = parityRecord{hash: ph, nodeID: parityNodes[i].id}
	}

	// create erasure group record
	result, err := g.db.ExecContext(ctx,
		"INSERT INTO erasure_group (data_shards, parity_shards, shard_size) VALUES (?, ?, ?)",
		k, m, shardSize)
	if err != nil {
		return err
	}
	groupID, err := result.LastInsertId()
	if err != nil {
		return err
	}

	// insert members: data shards
	for i, ci := range dataChunks {
		_, err := g.db.ExecContext(ctx,
			"INSERT INTO erasure_group_member (hash_id, erasure_group_id, slice_idx) VALUES (?, ?, ?)",
			ci.hash[:], groupID, i)
		if err != nil {
			return err
		}
	}
	// insert members: parity shards
	for i, pr := range parityRecords {
		_, err := g.db.ExecContext(ctx,
			"INSERT INTO erasure_group_member (hash_id, erasure_group_id, slice_idx) VALUES (?, ?, ?)",
			pr.hash[:], groupID, k+i)
		if err != nil {
			return err
		}
	}

	return nil
}

// Repair implements [GCAS].
// It scans all erasure groups for missing or corrupt shards and reconstructs them
// onto available nodes.
func (g *GcasImpl) Repair(ctx context.Context) error {
	type groupRow struct {
		id           int64
		dataShards   int
		parityShards int
		shardSize    int
	}

	groupRows, err := g.db.QueryContext(ctx,
		"SELECT id, data_shards, parity_shards, shard_size FROM erasure_group")
	if err != nil {
		return err
	}

	var groups []groupRow
	for groupRows.Next() {
		var gr groupRow
		if err := groupRows.Scan(&gr.id, &gr.dataShards, &gr.parityShards, &gr.shardSize); err != nil {
			groupRows.Close()
			return err
		}
		groups = append(groups, gr)
	}
	groupRows.Close()

	for _, gr := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := g.repairGroup(ctx, gr.id, gr.dataShards, gr.parityShards, gr.shardSize); err != nil {
			log.Printf("Repair: group %d: %v", gr.id, err)
		}
	}
	return nil
}

func (g *GcasImpl) repairGroup(ctx context.Context, groupID int64, dataShards, pShards, shardSize int) error {
	type memberInfo struct {
		sliceIdx int
		hash     Hash
		size     int
		nodeID   string
	}

	rows, err := g.db.QueryContext(ctx, `
		SELECT egm.slice_idx, egm.hash_id, c.size, c.node_id
		FROM erasure_group_member egm
		JOIN chunks c ON c.hash = egm.hash_id
		WHERE egm.erasure_group_id = ?
		ORDER BY egm.slice_idx`, groupID)
	if err != nil {
		return err
	}
	defer rows.Close()

	members := make([]memberInfo, 0, dataShards+pShards)
	for rows.Next() {
		var mi memberInfo
		var hashBytes []byte
		if err := rows.Scan(&mi.sliceIdx, &hashBytes, &mi.size, &mi.nodeID); err != nil {
			return err
		}
		if len(hashBytes) != len(Hash{}) {
			continue
		}
		copy(mi.hash[:], hashBytes)
		members = append(members, mi)
	}
	rows.Close()

	g.nodesLock.RLock()
	nodesCopy := make(map[string]CAS, len(g.nodes))
	for id, cas := range g.nodes {
		nodesCopy[id] = cas
	}
	g.nodesLock.RUnlock()

	// try to read each shard
	total := dataShards + pShards
	shards := make([][]byte, total)
	broken := make([]bool, total)

	for _, mi := range members {
		cas, ok := nodesCopy[mi.nodeID]
		if !ok {
			broken[mi.sliceIdx] = true
			continue
		}
		data, err := cas.Get(ctx, mi.hash)
		if err != nil {
			broken[mi.sliceIdx] = true
			continue
		}
		padded := make([]byte, shardSize)
		copy(padded, data)
		shards[mi.sliceIdx] = padded
	}

	// count broken shards
	brokenCount := 0
	for _, b := range broken {
		if b {
			brokenCount++
		}
	}
	if brokenCount == 0 {
		return nil // all good
	}

	present := total - brokenCount
	if present < dataShards {
		return fmt.Errorf("unrecoverable: only %d/%d shards present", present, dataShards)
	}

	// allocate nil shards so reedsolomon knows which to reconstruct
	for i, b := range broken {
		if b {
			shards[i] = nil
		}
	}

	enc, err := reedsolomon.New(dataShards, pShards)
	if err != nil {
		return err
	}
	if err := enc.Reconstruct(shards); err != nil {
		return err
	}

	// build set of nodes currently used in this stripe
	usedNodes := make(map[string]bool, total)
	for _, mi := range members {
		if !broken[mi.sliceIdx] {
			usedNodes[mi.nodeID] = true
		}
	}

	// store recovered shards
	for _, mi := range members {
		if !broken[mi.sliceIdx] {
			continue
		}

		shard := shards[mi.sliceIdx]
		origSize := mi.size
		if mi.sliceIdx >= dataShards {
			// parity shard: store full padded size
			origSize = shardSize
		}
		reconstructed := shard[:origSize]

		// find a node not already in the stripe
		var targetID string
		var targetCAS CAS
		for id, cas := range nodesCopy {
			if !usedNodes[id] {
				targetID = id
				targetCAS = cas
				break
			}
		}
		if targetCAS == nil {
			// fall back: try to re-use the original node if it's now connected
			if cas, ok := nodesCopy[mi.nodeID]; ok {
				targetID = mi.nodeID
				targetCAS = cas
			}
		}
		if targetCAS == nil {
			log.Printf("Repair: no available node for shard %d of group %d", mi.sliceIdx, groupID)
			continue
		}

		g.shardedLocker.Lock(mi.hash)
		err := targetCAS.Put(ctx, mi.hash, reconstructed)
		if err != nil && !errors.Is(err, HashExistsError{}) {
			g.shardedLocker.Unlock(mi.hash)
			log.Printf("Repair: put shard %d: %v", mi.sliceIdx, err)
			continue
		}
		_, dbErr := g.db.ExecContext(ctx,
			"UPDATE chunks SET node_id = ? WHERE hash = ?",
			targetID, mi.hash[:])
		g.shardedLocker.Unlock(mi.hash)
		if dbErr != nil {
			return dbErr
		}

		usedNodes[targetID] = true
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

	if err := g.formStripes(ctx); err != nil {
		log.Printf("error while forming stripes: %v", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := g.Repair(ctx); err != nil {
		log.Printf("error while repairing: %v", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	return nil
}

var _ GCAS = (*GcasImpl)(nil)
