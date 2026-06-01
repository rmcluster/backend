package gcas

import (
	"context"
	"fmt"
	"strings"
)

type DeviceDisplay struct {
	DisplayName string
}

func (g *GcasImpl) DevicesForHashes(ctx context.Context, hashes []Hash) ([]DeviceDisplay, error) {
	if len(hashes) == 0 {
		return nil, nil
	}

	placeholders := make([]string, 0, len(hashes))
	args := make([]any, 0, len(hashes))
	for _, hash := range hashes {
		placeholders = append(placeholders, "?")
		args = append(args, hash[:])
	}

	query := fmt.Sprintf(`
		SELECT c.node_id, COALESCE(dm.display_name, '')
		FROM chunks c
		LEFT JOIN device_metadata dm ON dm.node_id = c.node_id
		WHERE c.is_data = 1 AND c.hash IN (%s)
		GROUP BY c.node_id, COALESCE(dm.display_name, '')
	`, strings.Join(placeholders, ","))

	rows, err := g.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var devices []DeviceDisplay
	for rows.Next() {
		var (
			nodeID      string
			displayName string
		)
		if err := rows.Scan(&nodeID, &displayName); err != nil {
			return nil, err
		}
		if strings.TrimSpace(nodeID) == "" {
			continue
		}

		displayName = strings.TrimSpace(displayName)
		if displayName == "" {
			displayName = "Unknown device"
		}

		devices = append(devices, DeviceDisplay{
			DisplayName: displayName,
		})
	}

	return devices, rows.Err()
}
