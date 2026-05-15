package scheduling

import "sync"

// LoadingStatusTracker receives phase callbacks from the InstanceFactory and
// exposes the current loading state via LoadingStatusProvider. It is decoupled
// from any specific scheduler implementation.
type LoadingStatusTracker struct {
	mu       sync.Mutex
	model    string
	phase    string
	progress float64
}

// OnPhaseUpdate is used as the SetPhaseCallback target on an InstanceFactory.
func (t *LoadingStatusTracker) OnPhaseUpdate(model, phase string, progress float64) {
	t.mu.Lock()
	if phase == PhaseReady {
		t.model = ""
		t.phase = ""
		t.progress = 0
	} else {
		t.model = model
		t.phase = phase
		t.progress = progress
	}
	t.mu.Unlock()
}

// GetLoadingStatus implements [LoadingStatusProvider].
func (t *LoadingStatusTracker) GetLoadingStatus() (model, phase string, progress float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.model, t.phase, t.progress
}

var _ LoadingStatusProvider = (*LoadingStatusTracker)(nil)
