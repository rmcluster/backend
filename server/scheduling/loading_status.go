package scheduling

// LoadingPhase describes the current startup phase of a model instance.
type LoadingPhase = string

const (
	PhaseStarting     LoadingPhase = "starting"
	PhaseInitializing LoadingPhase = "initializing"
	PhaseDownloading  LoadingPhase = "downloading"
	PhaseLoading      LoadingPhase = "loading_model"
	PhaseWarmingUp    LoadingPhase = "warming_up"
	PhaseReady        LoadingPhase = "ready"
)

// LoadingStatusProvider is optionally implemented by a Scheduler to expose
// the current model loading progress to HTTP handlers.
type LoadingStatusProvider interface {
	// GetLoadingStatus returns the active model name, its current phase,
	// download progress in [0,100] (meaningful only when phase == PhaseDownloading),
	// and the number of layers offloaded to remote GPU nodes (0 until known).
	GetLoadingStatus() (model, phase string, progress float64, layersOnRpc int)
}

// LoadingStatusBroadcaster optionally allows listeners to subscribe to loading
// state changes for richer UI experiences such as resumable per-chat progress.
type LoadingStatusBroadcaster interface {
	LoadingStatusProvider
	Subscribe(func(scopeID, model, phase string, progress float64, layersOnRpc int)) func()
}

// PhaseCallbackSetter is optionally implemented by an InstanceFactory so
// the scheduler can register a hook that fires whenever the loading phase changes.
type PhaseCallbackSetter interface {
	SetPhaseCallback(func(scopeID, model, phase string, progress float64))
	SetLayersCallback(func(scopeID string, layersOnRpc int))
}
