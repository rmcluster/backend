package scheduling

import "sync"

type loadingStatusState struct {
	model       string
	phase       string
	progress    float64
	layersOnRpc int
}

// LoadingStatusTracker receives phase callbacks from the InstanceFactory and
// exposes the current loading state via LoadingStatusProvider. It is decoupled
// from any specific scheduler implementation.
type LoadingStatusTracker struct {
	mu          sync.Mutex
	latestScope string
	scopes      map[string]loadingStatusState
	listenerID  int
	listeners   map[int]func(scopeID, model, phase string, progress float64, layersOnRpc int)
}

// OnPhaseUpdate is used as the SetPhaseCallback target on an InstanceFactory.
func (t *LoadingStatusTracker) OnPhaseUpdate(scopeID, model, phase string, progress float64) {
	t.mu.Lock()
	if phase == PhaseReady {
		if t.scopes != nil {
			delete(t.scopes, scopeID)
		}
		if t.latestScope == scopeID {
			t.latestScope = ""
		}
	} else {
		if t.scopes == nil {
			t.scopes = make(map[string]loadingStatusState)
		}
		state := t.scopes[scopeID]
		state.model = model
		state.phase = phase
		state.progress = progress
		t.scopes[scopeID] = state
		t.latestScope = scopeID
	}
	listeners := t.snapshotListenersLocked()
	current := t.scopeSnapshotLocked(scopeID)
	t.mu.Unlock()
	t.notifyListeners(listeners, scopeID, current.model, current.phase, current.progress, current.layersOnRpc)
}

// OnLayersKnown is called when llama.cpp reports how many layers were offloaded.
func (t *LoadingStatusTracker) OnLayersKnown(scopeID string, layersOnRpc int) {
	t.mu.Lock()
	if t.scopes == nil {
		t.scopes = make(map[string]loadingStatusState)
	}
	state := t.scopes[scopeID]
	state.layersOnRpc = layersOnRpc
	t.scopes[scopeID] = state
	t.latestScope = scopeID
	listeners := t.snapshotListenersLocked()
	current := t.scopeSnapshotLocked(scopeID)
	t.mu.Unlock()
	t.notifyListeners(listeners, scopeID, current.model, current.phase, current.progress, current.layersOnRpc)
}

// GetLoadingStatus implements [LoadingStatusProvider].
func (t *LoadingStatusTracker) GetLoadingStatus() (model, phase string, progress float64, layersOnRpc int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if state, ok := t.currentStateLocked(); ok {
		return state.model, state.phase, state.progress, state.layersOnRpc
	}
	return "", "", 0, 0
}

// Subscribe registers a listener for every loading-state change and returns an
// unsubscribe function.
func (t *LoadingStatusTracker) Subscribe(listener func(scopeID, model, phase string, progress float64, layersOnRpc int)) func() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listeners == nil {
		t.listeners = make(map[int]func(scopeID, model, phase string, progress float64, layersOnRpc int))
	}
	t.listenerID++
	id := t.listenerID
	t.listeners[id] = listener
	return func() {
		t.mu.Lock()
		delete(t.listeners, id)
		t.mu.Unlock()
	}
}

func (t *LoadingStatusTracker) currentStateLocked() (loadingStatusState, bool) {
	if t.latestScope != "" {
		if state, ok := t.scopes[t.latestScope]; ok {
			return state, true
		}
	}
	for scopeID, state := range t.scopes {
		t.latestScope = scopeID
		return state, true
	}
	return loadingStatusState{}, false
}

func (t *LoadingStatusTracker) scopeSnapshotLocked(scopeID string) loadingStatusState {
	if state, ok := t.scopes[scopeID]; ok {
		return state
	}
	return loadingStatusState{}
}

func (t *LoadingStatusTracker) snapshotListenersLocked() []func(scopeID, model, phase string, progress float64, layersOnRpc int) {
	if len(t.listeners) == 0 {
		return nil
	}
	listeners := make([]func(scopeID, model, phase string, progress float64, layersOnRpc int), 0, len(t.listeners))
	for _, listener := range t.listeners {
		listeners = append(listeners, listener)
	}
	return listeners
}

func (t *LoadingStatusTracker) notifyListeners(
	listeners []func(scopeID, model, phase string, progress float64, layersOnRpc int),
	scopeID, model, phase string,
	progress float64,
	layersOnRpc int,
) {
	for _, listener := range listeners {
		listener(scopeID, model, phase, progress, layersOnRpc)
	}
}

var _ LoadingStatusProvider = (*LoadingStatusTracker)(nil)
var _ LoadingStatusBroadcaster = (*LoadingStatusTracker)(nil)
