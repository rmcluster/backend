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
	Ip() string
	Port() int
	MaxSize() int64
}

type Task interface {
	Model() string
	PerformInference(instance Instance) error
}
