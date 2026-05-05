package scheduling

// LoadingPhase describes the current startup phase of a model instance.
type LoadingPhase = string

const (
	PhaseStarting    LoadingPhase = "starting"
	PhaseDownloading LoadingPhase = "downloading"
	PhaseLoading     LoadingPhase = "loading_model"
	PhaseWarmingUp   LoadingPhase = "warming_up"
)

// LoadingStatusProvider is optionally implemented by a Scheduler to expose
// the current model loading progress to HTTP handlers.
type LoadingStatusProvider interface {
	GetLoadingStatus() (model, phase string)
}

// PhaseCallbackSetter is optionally implemented by an InstanceFactory so
// the scheduler can register a hook that fires whenever the loading phase changes.
type PhaseCallbackSetter interface {
	SetPhaseCallback(func(model, phase string))
}
