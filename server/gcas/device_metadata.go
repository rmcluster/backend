package gcas

import (
	"context"
	"strings"
	"time"
)

func (g *GcasImpl) UpsertDeviceMetadata(ctx context.Context, nodeID, displayName string) error {
	_, err := g.db.ExecContext(
		ctx,
		`INSERT INTO device_metadata(node_id, display_name, updated_at_ns)
		 VALUES (?, ?, ?)
		 ON CONFLICT(node_id) DO UPDATE SET
		 	display_name = excluded.display_name,
		 	updated_at_ns = excluded.updated_at_ns`,
		nodeID,
		strings.TrimSpace(displayName),
		time.Now().UnixNano(),
	)
	return err
}
