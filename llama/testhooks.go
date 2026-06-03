package llama

// Test hooks used only by tests to force error branches and improve coverage.
var (
	TestForceCacheListOutputError bool
	TestForceCacheParseError      bool
	TestForceInspectPipeError     bool
	TestForceInspectStartError    bool
)
