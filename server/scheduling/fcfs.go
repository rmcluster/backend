package scheduling

import (
	"fmt"
	"log"
	"sync"
)

type schedulerState int

const (
	stateIdle schedulerState = iota
	stateStarting
	stateRunning
)

// FcfsScheduler implements [Scheduler] using a first-come-first-served policy.
type FcfsScheduler struct {
	mu              sync.Mutex
	factory         InstanceFactory
	nodes           map[string]Node
	queue           []Task
	state           schedulerState
	activeModel     string
	activeInst      Instance
	runningTasks    int
	loadingPhase    string  // set while state == stateStarting
	loadingProgress float64 // download progress [0,100], only meaningful during PhaseDownloading
}

// NewFcfsScheduler creates a new FcfsScheduler.
func NewFcfsScheduler(factory InstanceFactory) *FcfsScheduler {
	return &FcfsScheduler{
		factory: factory,
		nodes:   make(map[string]Node),
	}
}

// OnInstanceDeath implements [Scheduler].
func (f *FcfsScheduler) OnInstanceDeath(instance Instance, exitCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.activeInst == instance {
		f.activeInst = nil
		f.state = stateIdle
		f.runningTasks = 0
		f.activeModel = ""
		f.pump()
	}
}

// OnNewTask implements [Scheduler].
func (f *FcfsScheduler) OnNewTask(task Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queue = append(f.queue, task)
	f.pump()
}

// OnNodeConnect implements [Scheduler].
func (f *FcfsScheduler) OnNodeConnect(node Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[node.Id()] = node
}

// OnNodeDisconnect implements [Scheduler].
func (f *FcfsScheduler) OnNodeDisconnect(node Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.nodes, node.Id())
}

// OnTaskCancellation implements [Scheduler].
func (f *FcfsScheduler) OnTaskCancellation(instance Instance, task Task) {
	f.OnTaskCompletion(instance, task)
}

// OnTaskCancelled implements [Scheduler].
func (f *FcfsScheduler) OnTaskCancelled(task Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, t := range f.queue {
		if t == task {
			f.queue = append(f.queue[:i], f.queue[i+1:]...)
			break
		}
	}
	f.pump()
}

// OnTaskCompletion implements [Scheduler].
func (f *FcfsScheduler) OnTaskCompletion(instance Instance, task Task) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.activeInst == instance {
		f.runningTasks--
		f.pump()
	}
}

func (f *FcfsScheduler) getNodes() []Node {
	nodes := make([]Node, 0, len(f.nodes))
	for _, n := range f.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

func (f *FcfsScheduler) pump() {
	if len(f.queue) == 0 {
		return
	}

	firstTask := f.queue[0]
	model := firstTask.Model()

	if f.state == stateIdle {
		f.state = stateStarting
		f.activeModel = model
		go f.startInstance(model, f.getNodes())
		return
	}

	if f.state == stateStarting {
		return
	}

	if f.state == stateRunning {
		if f.activeModel == model {
			// Model matches, we can start the task!
			task := f.queue[0]
			f.queue = f.queue[1:]
			f.runningTasks++

			go func(t Task, inst Instance) {
				_ = t.PerformInference(inst)
				f.OnTaskCompletion(inst, t)
			}(task, f.activeInst)

			// Continue pumping to check if we can start more tasks
			f.pump()
		} else {
			// Need a different model.
			if f.runningTasks == 0 {
				// Old instance is idle, stop it and start new one
				f.activeInst.Stop()
				f.activeInst = nil
				f.state = stateStarting
				f.activeModel = model
				go f.startInstance(model, f.getNodes())
			}
		}
	}
}

// GetLoadingStatus implements [LoadingStatusProvider].
func (f *FcfsScheduler) GetLoadingStatus() (model, phase string, progress float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state != stateStarting {
		return "", "", 0
	}
	return f.activeModel, f.loadingPhase, f.loadingProgress
}

func (f *FcfsScheduler) startInstance(model string, nodes []Node) {
	// Mark initial phase and register a callback so stderr lines update it.
	f.mu.Lock()
	f.loadingPhase = PhaseStarting
	f.mu.Unlock()

	if setter, ok := f.factory.(PhaseCallbackSetter); ok {
		setter.SetPhaseCallback(func(m, phase string, progress float64) {
			f.mu.Lock()
			if f.activeModel == m {
				f.loadingPhase = phase
				f.loadingProgress = progress
			}
			f.mu.Unlock()
		})
	}

	var err error

	// refuse to start if no nodes are available
	if len(nodes) == 0 {
		err = fmt.Errorf("no nodes available")
	}

	var instance Instance
	if err == nil {
		instance, err = f.factory.StartInstance(model, nodes)
	}

	if err == nil {
		log.Printf("FcfsScheduler: Waiting for instance for model %s to be ready...", model)
		err = instance.WaitReady()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err != nil {
		log.Printf("FcfsScheduler: Failed to start instance for model %s: %v", model, err)
		f.state = stateIdle
		f.loadingPhase = ""
		// Drop the failing task to prevent an infinite retry loop, and notify
		// the waiting handler so its HTTP request doesn't hang forever.
		if len(f.queue) > 0 && f.queue[0].Model() == model {
			failedTask := f.queue[0]
			f.queue = f.queue[1:]
			go func(t Task, e error) {
				type failable interface{ Fail(error) }
				if ft, ok := t.(failable); ok {
					ft.Fail(e)
				}
			}(failedTask, err)
		}
	} else {
		f.state = stateRunning
		f.loadingPhase = ""
		f.activeInst = instance
		go func(inst Instance) {
			inst.AwaitTermination()
			f.OnInstanceDeath(inst, -1)
		}(instance)
	}

	f.pump()
}

var _ Scheduler = (*FcfsScheduler)(nil)
