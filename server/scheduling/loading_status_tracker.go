package scheduling

import "sync"

// LoadingStatusTracker receives phase callbacks from the InstanceFactory and
// exposes the current loading state via LoadingStatusProvider. It is decoupled
// from any specific scheduler implementation.
type LoadingStatusTracker struct {
	mu          sync.Mutex
	model       string
	phase       string
	progress    float64
	layersOnGpu int
	loadedByModel map[string][]LoadedDevice
}

// OnPhaseUpdate is used as the SetPhaseCallback target on an InstanceFactory.
func (t *LoadingStatusTracker) OnPhaseUpdate(model, phase string, progress float64) {
	t.mu.Lock()
	if t.loadedByModel == nil {
		t.loadedByModel = make(map[string][]LoadedDevice)
	}
	if phase == PhaseReady {
		t.model = ""
		t.phase = ""
		t.progress = 0
		t.layersOnGpu = 0
	} else {
		t.model = model
		t.phase = phase
		t.progress = progress
	}
	t.mu.Unlock()
}

// OnLayersKnown is called when llama.cpp reports how many layers were offloaded.
func (t *LoadingStatusTracker) OnLayersKnown(layersOnGpu int) {
	t.mu.Lock()
	t.layersOnGpu = layersOnGpu
	t.mu.Unlock()
}
func (t *LoadingStatusTracker) OnLoadedDevicesChanged(model string, devices []LoadedDevice) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.loadedByModel == nil {
		t.loadedByModel = make(map[string][]LoadedDevice)
	}
	if len(devices) == 0 {
		delete(t.loadedByModel, model)
		return
	}
	t.loadedByModel[model] = append([]LoadedDevice(nil), devices...)
}

// GetLoadingStatus implements [LoadingStatusProvider].
func (t *LoadingStatusTracker) GetLoadingStatus() (model, phase string, progress float64, layersOnGpu int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.model, t.phase, t.progress, t.layersOnGpu
}

func (t *LoadingStatusTracker) GetLoadedDevices(model string) []LoadedDevice {
	t.mu.Lock()
	defer t.mu.Unlock()
	devices := t.loadedByModel[model]
	if devices == nil {
		return make([]LoadedDevice, 0)
	}
	return append([]LoadedDevice(nil), devices...)
}

var _ LoadingStatusProvider = (*LoadingStatusTracker)(nil)
var _ LoadedDevicesProvider = (*LoadingStatusTracker)(nil)
