package scheduling

import (
	"bytes"
	"context"
	"log"
	"os/exec"
	"strings"
	"sync"

	"github.com/wk-y/rama-swap/llama"
)

func NewInstanceFactory(llmService *llama.Llama, lowestPort int) InstanceFactory {
	return &instanceFactoryImpl{
		llmService: llmService,
		lowestPort: lowestPort,
		usedPorts:  make(map[int]struct{}),
	}
}

type instanceFactoryImpl struct {
	sync.Mutex
	llmService    *llama.Llama
	lowestPort    int              // the lowest port to use
	usedPorts     map[int]struct{} // ports that are currently in use
	phaseCallback func(model, phase string)
}

// SetPhaseCallback implements [PhaseCallbackSetter].
func (i *instanceFactoryImpl) SetPhaseCallback(cb func(model, phase string)) {
	i.Lock()
	defer i.Unlock()
	i.phaseCallback = cb
}

// StartInstance implements [InstanceFactory].
func (i *instanceFactoryImpl) StartInstance(model string, nodes []Node) (Instance, error) {
	log.Printf("Starting instance for model %s on %d nodes", model, len(nodes))

	// build list of rpc nodes
	rpcNodes := make([]llama.RpcNode, len(nodes))
	for idx, node := range nodes {
		rpcNodes[idx] = llama.RpcNode{
			Ip:   node.Ip(),
			Port: node.Port(),
		}
	}

	// find the lowest port that is not used
	port := i.lowestPort
	for {
		if _, ok := i.usedPorts[port]; !ok {
			break
		}
		port++
	}

	// start the llama server
	cmd, err := func() (*exec.Cmd, error) {
		// guard critical section
		i.Lock()
		defer i.Unlock()
		offloadLayers := chooseOffloadLayers(nodes)

		cmd := i.llmService.ServeCommand(context.Background(), llama.ServeArgs{
			Model:         model,
			RpcNodes:      rpcNodes,
			Port:          port,
			OffloadLayers: &offloadLayers,
		})
		cmd.Stdout = newProcessLogWriter(model, "stdout", nil)
		// i is already locked here — read phaseCallback directly, no re-lock
		cmd.Stderr = newProcessLogWriter(model, "stderr", makePhaseDetector(model, i.phaseCallback))

		err := cmd.Start()
		if err != nil {
			return nil, err
		}

		// mark port as used
		i.usedPorts[port] = struct{}{}

		return cmd, err
	}()

	if err != nil {
		return nil, err
	}
	log.Printf("Started instance process for model %s on port %d", model, port)
	log.Printf("Instance command: %s", strings.Join(cmd.Args, " "))

	dead := make(chan struct{})

	// wait for instance to die, then free port
	go func() {
		cmd.Wait()
		close(dead)

		i.Lock()
		delete(i.usedPorts, port)
		i.Unlock()
	}()

	// return the new instance
	return &instanceImpl{
		process: cmd.Process,
		port:    port,
		dead:    dead,
		model:   model,
	}, nil
}

func chooseOffloadLayers(nodes []Node) int {
	const defaultOffloadLayers = 8
	const minRemoteBufferBytes = 256 * 1024 * 1024

	if len(nodes) == 0 {
		return 0
	}

	var smallestMaxSize int64 = -1
	for _, node := range nodes {
		maxSize := node.MaxSize()
		if maxSize <= 0 {
			return 0
		}
		if smallestMaxSize < 0 || maxSize < smallestMaxSize {
			smallestMaxSize = maxSize
		}
	}

	if smallestMaxSize < minRemoteBufferBytes {
		return 0
	}

	return defaultOffloadLayers
}

type processLogWriter struct {
	model  string
	stream string
	buffer bytes.Buffer
	onLine func(string) // called with each complete trimmed line
}

func newProcessLogWriter(model string, stream string, onLine func(string)) *processLogWriter {
	return &processLogWriter{model: model, stream: stream, onLine: onLine}
}

func (w *processLogWriter) Write(p []byte) (int, error) {
	w.buffer.Write(p)
	for {
		line, err := w.buffer.ReadString('\n')
		if err == bytes.ErrTooLarge {
			break
		}
		if err != nil {
			if w.buffer.Len() == 0 {
				break
			}
			remaining := strings.TrimSpace(w.buffer.String())
			if remaining != "" {
				log.Printf("[llama %s %s] %s", w.model, w.stream, remaining)
				if w.onLine != nil {
					w.onLine(remaining)
				}
			}
			w.buffer.Reset()
			break
		}
		line = strings.TrimRight(line, "\n")
		line = strings.TrimRight(line, "\r")
		if line != "" {
			log.Printf("[llama %s %s] %s", w.model, w.stream, line)
			if w.onLine != nil {
				w.onLine(line)
			}
		}
	}
	return len(p), nil
}

// makePhaseDetector returns an onLine hook that calls cb whenever a known
// loading phase is detected in a llama-server stderr line.
func makePhaseDetector(model string, cb func(model, phase string)) func(string) {
	if cb == nil {
		return nil
	}
	return func(line string) {
		var phase string
		switch {
		case strings.Contains(line, ": downloading from "):
			phase = PhaseDownloading
		case strings.Contains(line, "load_model: loading model"),
			strings.Contains(line, "main: loading model"):
			phase = PhaseLoading
		case strings.Contains(line, "warming up"):
			phase = PhaseWarmingUp
		}
		if phase != "" {
			cb(model, phase)
		}
	}
}

var _ InstanceFactory = (*instanceFactoryImpl)(nil)
