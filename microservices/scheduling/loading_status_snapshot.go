package scheduling

import "time"

// OffloadDeviceStatus describes one device participating in model offload.
// The current UI only needs a lightweight, serializable shape here.
type OffloadDeviceStatus struct {
	NodeID string `json:"node_id,omitempty"`
	Label  string `json:"label,omitempty"`
	Layers int    `json:"layers,omitempty"`
}

// LoadingStatusSnapshot is the full loading/debug state exposed to the UI.
type LoadingStatusSnapshot struct {
	Model              string                `json:"model"`
	Phase              string                `json:"phase"`
	Progress           float64               `json:"progress"`
	LayersOnGpu        int                   `json:"layers_on_gpu"`
	OffloadDevices     []OffloadDeviceStatus `json:"offload_devices,omitempty"`
	HostOffloadEnabled bool                  `json:"host_offload_enabled"`
	Detail             string                `json:"detail,omitempty"`
	Error              string                `json:"error,omitempty"`
	LastLogLine        string                `json:"last_log_line,omitempty"`
	WaitingForNodes    bool                  `json:"waiting_for_nodes"`
	NodesAvailable     int                   `json:"nodes_available"`
	NodesNeeded        int                   `json:"nodes_needed"`
	UpdatedAt          time.Time             `json:"updated_at,omitempty"`
}
