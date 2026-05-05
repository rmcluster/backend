package server

import "github.com/wk-y/rama-swap/microservices/scheduling"

type taskWithCompletion struct {
	inner scheduling.Task
	done  chan struct{}
}

// Model implements [scheduling.Task].
func (t *taskWithCompletion) Model() string {
	return t.inner.Model()
}

// PerformInference implements [scheduling.Task].
func (t *taskWithCompletion) PerformInference(instance scheduling.Instance) error {
	defer close(t.done)
	return t.inner.PerformInference(instance)
}

// Fail signals the waiting handler that no instance could be started.
func (t *taskWithCompletion) Fail(err error) {
	defer close(t.done)
	if f, ok := t.inner.(interface{ Fail(error) }); ok {
		f.Fail(err)
	}
}

func newTaskWithCompletion(task scheduling.Task) *taskWithCompletion {
	return &taskWithCompletion{
		inner: task,
		done:  make(chan struct{}),
	}
}

var _ scheduling.Task = (*taskWithCompletion)(nil)
