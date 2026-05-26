package scheduling

import (
	"net/http/httputil"

	"github.com/openai/openai-go/v2"
)

type Instance interface {
	Model() string
	GetOpenAIClient() openai.Client
	ReverseProxy() *httputil.ReverseProxy
	WaitReady() error // block until instance is ready to serve requests. note that the instance may die after returning
	Stop()
	Kill()
	AwaitTermination()
}

type InstanceFactory interface {
	StartInstance(model string, nodes []Node) (Instance, error)
}

type Node interface {
	Id() string
	HardwareModel() string
	Ip() string
	Port() int
	MaxSize() int64
}

type Task interface {
	Model() string
	PerformInference(instance Instance) error
	Fail(err error)
}

// AllocatedNodesAwareTask is an optional extension that lets the scheduler
// attach the concrete nodes assigned to a task before inference begins.
type AllocatedNodesAwareTask interface {
	Task
	SetAllocatedNodes([]Node)
}

// BenchmarkGroupAwareTask lets the scheduler keep paired benchmark requests on
// the same warmed instance when possible.
type BenchmarkGroupAwareTask interface {
	Task
	BenchmarkGroupID() string
	BenchmarkStage() string
}
